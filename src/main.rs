use std::io::{Read, Write};
use std::collections::{HashMap, HashSet};
use std::ffi::OsStr;
use std::ops::Deref;
use std::str::FromStr;
use std::sync::{Arc, RwLock, mpsc};
use anyhow::{anyhow, Context};
use fast_image_resize as fr;
use interprocess::os::windows::named_pipe::{PipeListener, DuplexBytePipeStream, PipeListenerOptions, PipeMode};
use image::{ImageFormat, GenericImageView, EncodableLayout, DynamicImage, GenericImage, GrayImage};
use sysinfo::{System, RefreshKind, MemoryRefreshKind};
use once_cell::sync::OnceCell;
use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

const GETS_THREAD_COUNT: usize = 12;
const BUFFER_SIZE: usize = 4096;
const PIPE_NAME: &str = "img_process_server";
const MIN_AVAILABLE_MEMORY : u64 = 6;

static CACHE_DIR : OnceCell<String> = OnceCell::new();
static THREADED_READS: OnceCell<bool> = OnceCell::new();
static FILL_STRATEGY: OnceCell<FillStrategy> = OnceCell::new();
static FILTER_TYPE: OnceCell<MyFilterType> = OnceCell::new();
static GRAYSCALE: OnceCell<bool> = OnceCell::new();

enum CacheType {
    OnDisk(u32),
    InMemory(Arc<Vec<u8>>)
}
type CachedImages = HashMap<String, CacheType>;
type CachedPaths = HashSet<String>;
type CachedImageShared = Arc<RwLock<(CachedImages, CachedPaths)>>;
type ThreadChannels = Vec<(mpsc::Sender<(u32, u32, Vec<String>)>, mpsc::Receiver<Vec<u8>>)>;


fn send_image<S: Write>(img_bytes : Arc<Vec<u8>>, stream : &mut S) -> anyhow::Result<()> {
    #[cfg(feature = "log")]
    let instant = std::time::Instant::now();
    stream.write(&(img_bytes.len() as u32).to_be_bytes())?; // Length
    stream.write(&img_bytes)?;
    #[cfg(feature = "log")]
    println!("s: {}ns", instant.elapsed().as_nanos());
    Ok(())
}

fn get_disk_cache_path(cache_id: &u32) -> anyhow::Result<String> {
    Ok(format!("{}/{}.bmp", CACHE_DIR.get().ok_or(anyhow!("Not setup"))?, cache_id))
}

fn cache_img(path : String, img_bytes : Arc<Vec<u8>>, cached_images : &CachedImageShared) -> anyhow::Result<()> {
    #[cfg(feature = "log")]
    let instant = std::time::Instant::now();
    let sys = System::new_with_specifics(RefreshKind::nothing()
        .with_memory(MemoryRefreshKind::everything())
    );
    let available_memory = sys.available_memory();
    let mut unlocked_cache = cached_images.write().expect("Cannot write to cache");

    if (available_memory / 1000000000) < MIN_AVAILABLE_MEMORY {
        let cache_id = unlocked_cache.0.len() as u32;
        std::fs::write(get_disk_cache_path(&cache_id)?, img_bytes.as_bytes()).context("Could not write to disk cache")?;
        unlocked_cache.0.insert(path, CacheType::OnDisk(cache_id));
    } else {
        unlocked_cache.0.insert(path, CacheType::InMemory(img_bytes));
    }
    #[cfg(feature = "log")]
    println!("ci: {}ns", instant.elapsed().as_nanos());
    Ok(())
}


fn get_from_cache<'a>(path : &str, cached_images : &CachedImageShared) -> anyhow::Result<Option<Arc<Vec<u8>>>> {
    let unlocked_cache = cached_images.read().expect("Cannot read from cache");
    let i = Ok(match unlocked_cache.0.get(path) {
        Some(cache_type) => {
            Some(match cache_type {
                CacheType::OnDisk(cache_id) => {
                    Arc::new(std::fs::read(get_disk_cache_path(cache_id)?)?)
                },
                CacheType::InMemory(img_bytes) => img_bytes.clone()
            })
        },
        None => None
    });
    i
}


#[derive(Debug, Copy, Clone)]
enum FillStrategy {
    Constant(u8),
    Reflect,
    Nearest
}

// Implement parse from string
impl FromStr for FillStrategy {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut split = s.split(" ");
        let name = split.next().context("No fill strategy")?;
        match name {
            "constant" => {
                let value : u8 = split.next().context("Expected value")?.parse().context("Expected u8")?;
                Ok(FillStrategy::Constant(value))
            },
            "reflect" => Ok(FillStrategy::Reflect),
            "nearest" => Ok(FillStrategy::Nearest),
            _ => Err(anyhow!("Unknown fill strategy"))
        }

    }
}

#[derive(Debug, Copy, Clone)]
enum MyFilterType {
    Thumbnail,
    Resize(fr::ResizeAlg)
}

impl FromStr for MyFilterType {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Thumbnail" => Ok(MyFilterType::Thumbnail),
            "Nearest" => Ok(MyFilterType::Resize(fr::ResizeAlg::Nearest)),
            "Triangle" => Ok(MyFilterType::Resize(fr::ResizeAlg::Convolution(fr::FilterType::Bilinear))),
            "CatmullRom" => Ok(MyFilterType::Resize(fr::ResizeAlg::Convolution(fr::FilterType::CatmullRom))),
            "Gaussian" => Ok(MyFilterType::Resize(fr::ResizeAlg::Convolution(fr::FilterType::Gaussian))),
            "Box" => Ok(MyFilterType::Resize(fr::ResizeAlg::Convolution(fr::FilterType::Box))),
            "Hamming" => Ok(MyFilterType::Resize(fr::ResizeAlg::Convolution(fr::FilterType::Hamming))),
            "Mitchell" => Ok(MyFilterType::Resize(fr::ResizeAlg::Convolution(fr::FilterType::Mitchell))),
            "Lanczos3" => Ok(MyFilterType::Resize(fr::ResizeAlg::Convolution(fr::FilterType::Lanczos3))),
            _ => Err(anyhow!("Unknown filter type"))
        }
    }
}


#[inline]
fn get_luma(r: u8, g: u8, b: u8) -> u32 {
    ((r as u32 * 77) + (g as u32 * 150) + (b as u32 * 29)) >> 8
}

fn to_grayscale(img: DynamicImage) -> DynamicImage {
    if let DynamicImage::ImageLuma8(_) = img {
        return img;
    }
    let DynamicImage::ImageRgb8(rgb8) = img else {
        return img.grayscale();
    };
    let width = rgb8.width();
    let height = rgb8.height();

    let src = rgb8.into_raw();
    let n = src.len() / 3;
    let mut gray = Vec::with_capacity(n);
    unsafe { gray.set_len(n) }; // SAFETY: we immediately initialize every element
    for (i, pixel) in src.chunks_exact(3).enumerate() {
        let y = get_luma(pixel[0], pixel[1], pixel[2]) as u8;
        unsafe {
            *gray.get_unchecked_mut(i) = y;
        }
    }
    DynamicImage::ImageLuma8(GrayImage::from_raw(width, height, gray).expect("Could not create gray image"))
}


fn resize(img: DynamicImage, width: u32, height: u32, filter_type: MyFilterType) -> DynamicImage {
    let resized = match filter_type {
        MyFilterType::Thumbnail => img.thumbnail(width, height),
        MyFilterType::Resize(resize_alg) => {
            let mut resizer = fr::Resizer::new();
            #[cfg(target_arch = "x86_64")]
            unsafe {
                resizer.set_cpu_extensions(fr::CpuExtensions::Avx2);
            }
            let resize_options = fr::ResizeOptions::new()
                .resize_alg(resize_alg);
            let mut resized = DynamicImage::new(width, height, img.color());
            resizer.resize(&img, &mut resized, &resize_options).expect("Could not resize image");
            resized
        }
    };
    resized
}


fn resize_and_pad(img: DynamicImage, width: u32, height: u32, fill_strategy: &FillStrategy, filter_type: MyFilterType) -> DynamicImage {
    let resized = resize(img, width, height, filter_type);
    let mut padded = DynamicImage::new_rgba8(width, height);
    let vertical_pad = resized.height() < height;

    let mid_y = (height - resized.height()) / 2;
    let mid_x = (width - resized.width()) / 2;

    match fill_strategy {
        FillStrategy::Constant(value) => {
            let p = padded.as_mut_rgba8().expect("Could not convert to RGBA8");
            p.as_mut().fill(*value)
        }
        FillStrategy::Reflect => {
            if vertical_pad {
                // Upper part
                for y in 0..mid_y {
                    let source_y = mid_y - y;
                    for x in 0..width {
                        let pixel = resized.get_pixel(x, source_y);
                        padded.put_pixel(x, y, pixel);
                    }
                }
                // Lower part
                let lower_start = mid_y + resized.height();
                for y in lower_start..height {
                    let source_y = (lower_start - y) + resized.height() - 1;
                    for x in 0..width {
                        let pixel = resized.get_pixel(x, source_y);
                        padded.put_pixel(x, y, pixel);
                    }
                }
            } else {
                // Left part
                for x in 0..mid_x {
                    let source_x = mid_x - x;
                    for y in 0..height {
                        let pixel = resized.get_pixel(source_x, y);
                        padded.put_pixel(x, y, pixel);
                    }
                }
                // Right part
                let right_start = mid_x + resized.width();
                for x in right_start..width {
                    let source_x = (right_start - x) + resized.width() - 1;
                    for y in 0..height {
                        let pixel = resized.get_pixel(source_x, y);
                        padded.put_pixel(x, y, pixel);
                    }
                }
            }
        },
        FillStrategy::Nearest => {
            if vertical_pad {
                let top_slice = resized.view(0, 0, width, 1);
                let bottom_slice = resized.view(0, resized.height() - 1, width, 1);
                for y in 0..mid_y {
                    padded.copy_from(top_slice.deref(), 0, y).expect("Cannot copy slice");
                }
                let lower_start = mid_y + resized.height();
                for y in lower_start..height {
                    padded.copy_from(bottom_slice.deref(), 0, y).expect("Cannot copy slice");
                }
            } else {
                let left_slice = resized.view(0, 0, 1, height);
                let right_slice = resized.view(resized.width() - 1, 0, 1, height);
                for x in 0..mid_x {
                    padded.copy_from(left_slice.deref(), x, 0).expect("Cannot copy slice");
                }
                let right_start = mid_x + resized.width();
                for x in right_start..width {
                    padded.copy_from(right_slice.deref(), x, 0).expect("Cannot copy slice");
                }
            }
        }
    }

    if vertical_pad {
        padded.copy_from(&resized, 0, mid_y).expect("Cannot copy to padded");
    } else {
        padded.copy_from(&resized, mid_x, 0).expect("Cannot copy to padded");
    }

    padded
}


fn get_image<S: Write>(stream : &mut S, cached_images : &CachedImageShared, path : &str, width : u32, height : u32) -> anyhow::Result<()> {
    match get_from_cache(path, cached_images)? {
        Some(img_bytes) => {
            send_image(img_bytes, stream)?;
            return Ok(());
        }
        None => {}
    };
    #[cfg(feature = "log")]
    let instant = std::time::Instant::now();
    let raw_img_bytes = if path.starts_with("https://") {
        reqwest::blocking::get(path)
            .with_context(|| format!("Error getting with path {}", path))?
            .error_for_status().context("Error status code")?
            .bytes().context("Could not get request bytes")?.to_vec()
    } else {
        if !*THREADED_READS.get().ok_or(anyhow!("Not setup"))? {
            // Just get the write guard first, which will prevent any other threads from reading images at the same time
            // This can improve performance, if reading off hard drives, because the seek head then doesn't have to move as much
            let _guard = cached_images.write().expect("Could not get write lock");
            std::fs::read(path).context("Could not read image file")?
            // guard gets dropped here
        } else {
            std::fs::read(path).context("Could not read image file")?
        }
    };
    #[cfg(feature = "log")]
    println!("r: {}ns", instant.elapsed().as_nanos());
    #[cfg(feature = "log")]
    let instant = std::time::Instant::now();

    let mut img = image::load_from_memory(&raw_img_bytes)?;
    if img.width() != width || img.height() != height {
        let fill_strategy = FILL_STRATEGY.get().context("Not setup")?;
        let filter_type = FILTER_TYPE.get().context("Not setup")?;
        let grayscale = GRAYSCALE.get().context("Not setup")?;
        if *grayscale {
            img = to_grayscale(img);
        }
        img = resize_and_pad(img, width, height, fill_strategy, *filter_type);
    }
    let mut bmp_img = std::io::Cursor::new(Vec::new());
    img.write_to(&mut bmp_img, ImageFormat::Bmp).context("could not convert image to bytes")?;
    let bmp_img_bytes = bmp_img.into_inner();
    #[cfg(feature = "log")]
    println!("d: {}ns", instant.elapsed().as_nanos());
    let bmp_img_bytes_rc = Arc::new(bmp_img_bytes);
    cache_img(path.to_string(), bmp_img_bytes_rc.clone(), cached_images)?;
    send_image(bmp_img_bytes_rc, stream)?;
    Ok(())
}

fn gets_thread(cached_images: CachedImageShared, receiver: mpsc::Receiver<(u32, u32, Vec<String>)>, sender: mpsc::Sender<Vec<u8>>) -> anyhow::Result<()> {
    loop {
        let (width, height, paths) = receiver.recv()?;
        let mut cursor = Vec::<u8>::new();
        for path in &paths {
            get_image(&mut cursor, &cached_images, path, width, height)?;
        }
        sender.send(cursor)?;
    }
}

fn gets_images(stream: &mut DuplexBytePipeStream, cached_images: &CachedImageShared, thread_channels: &ThreadChannels, width: u32, height: u32, paths: &[&str]) -> anyhow::Result<()> {
    let unlocked_cache = cached_images.read().expect("Cannot read from cache");
    let paths_key = paths.join("");
    #[cfg(feature = "log")]
    let instant = std::time::Instant::now();
    let all_cached = unlocked_cache.1.contains(&paths_key);
    #[cfg(feature = "log")]
    println!("uc: {}ns, e: {}", instant.elapsed().as_nanos(), unlocked_cache.1.len());
    std::mem::drop(unlocked_cache);
    if all_cached {
        for path in paths {
            get_image(stream, cached_images, path, width, height)?;
        }
        return Ok(());
    }

    let chunk_size = paths.len() / GETS_THREAD_COUNT;
    let mut thread_chunks : Vec<_> = if chunk_size == 0 {
        vec![paths.iter().map(|p| *p).collect()]
    } else {
        paths.chunks(chunk_size).map(|v| Vec::from(v)).collect()
    };
    while thread_chunks.len() > GETS_THREAD_COUNT {
        let mut remainder = thread_chunks.pop().unwrap();
        thread_chunks.last_mut().unwrap().append(&mut remainder);
    }
    for (i, thread_paths) in thread_chunks.iter().enumerate() {
        let thread_paths : Vec<_> = thread_paths.iter().map(|s| s.to_string()).collect();
        thread_channels[i].0.send((width, height, thread_paths))?;
    }

    for i in 0..thread_chunks.len() {
        let (_, receiver) = &thread_channels[i];
        let thread_written_data : Vec<u8> = receiver.recv()?;
        stream.write(&thread_written_data)?;
    }
    let mut unlocked_cache = cached_images.write().expect("Cannot read from cache");
    unlocked_cache.1.insert(paths_key);
    Ok(())
}


fn setup(disk_cache_dir: &str, working_dir: &str, threaded_reads: bool, fill_strategy: FillStrategy, filter_type: MyFilterType, grayscale: bool) -> anyhow::Result<()> {
    std::env::set_current_dir(working_dir)?;
    if CACHE_DIR.get().is_some() {return Ok(());}
    CACHE_DIR.set(disk_cache_dir.to_string()).expect("Can only setup once!");
    THREADED_READS.set(threaded_reads).unwrap();
    FILL_STRATEGY.set(fill_strategy).unwrap();
    FILTER_TYPE.set(filter_type).unwrap();
    GRAYSCALE.set(grayscale).unwrap();
    Ok(())
}

fn clear_cache(cached_images : &CachedImageShared) -> anyhow::Result<()> {
    let mut unlocked_cache = cached_images.write().expect("Cannot write to cache");
    let cached_paths = &mut unlocked_cache.1;
    cached_paths.clear();
    let cached_images = &mut unlocked_cache.0;
    for (_, cache_type) in cached_images.drain() {
        match cache_type {
            CacheType::OnDisk(cache_id) => {
                std::fs::remove_file(get_disk_cache_path(&cache_id)?)?
            },
            CacheType::InMemory(_) => {}
        }
    }
    Ok(())
}

fn process_command(args : &[&str], stream : &mut DuplexBytePipeStream, cached_images : &CachedImageShared, thread_channels: &ThreadChannels) -> anyhow::Result<()> {
    match args[0] {
        "clear_cache" => clear_cache(cached_images)?,
        "setup" => setup(args[1], args[2], args[3] == "true", args[4].parse()?, args[5].parse()?, args[6] == "true")?,
        "gets" => gets_images(stream, cached_images, thread_channels, args[1].parse::<u32>().unwrap(), args[2].parse::<u32>().unwrap(), &args[3..])?,
        "get" => {get_image(stream, cached_images, args[1], args[2].parse::<u32>().unwrap(), args[3].parse::<u32>().unwrap())?},
        _ => {println!("No such command : {}", args[0])}
    }
    Ok(())
}


fn read_command(stream : &mut DuplexBytePipeStream, cached_images : &CachedImageShared, thread_channels: &ThreadChannels) -> anyhow::Result<()> {
    let mut read_size_buffer = [0u8; 4];
    if stream.read(&mut read_size_buffer)? == 0 {return Ok(())};
    let msg_size = u32::from_be_bytes(read_size_buffer);
    let mut data = Vec::with_capacity(BUFFER_SIZE);
    let mut buff = [0u8;BUFFER_SIZE];
    while data.len() < msg_size as usize {
        let length = stream.read(&mut buff)?;
        data.extend(&buff[..length]);
    }

    let command = String::from_utf8_lossy(&data).into_owned();
    let args : Vec<&str> = command.split("|").collect();
    process_command(&args, stream, cached_images, thread_channels)?;
    Ok(())
}


fn read_loop(mut stream: DuplexBytePipeStream, cached_images: CachedImageShared, thread_channels: &ThreadChannels) -> anyhow::Result<()> {
    loop {
        read_command(&mut stream, &cached_images, thread_channels)?
    }
}


fn main() {
    let listener : PipeListener<DuplexBytePipeStream> = PipeListenerOptions::new()
        .name(OsStr::new(PIPE_NAME))
        .mode(PipeMode::Messages)
        .create()
        .expect("Could not create pipe listener");

    let data = HashMap::with_capacity(300000);
    let cached_images = CachedImageShared::new(RwLock::new((data, Default::default())));

    let mut thread_channels = Vec::with_capacity(GETS_THREAD_COUNT);
    for i in 0..GETS_THREAD_COUNT {
        let thread_cached_images = cached_images.clone();
        let (to_thread_send, in_thread_recv) = mpsc::channel();
        let (in_thread_send, from_thread_recv) = mpsc::channel();
        std::thread::spawn(move || {
            if let Err(e) = gets_thread(thread_cached_images, in_thread_recv, in_thread_send) {
                eprintln!("Thread {} exited with error: {}", i, e);
            }
        });
        thread_channels.push((to_thread_send, from_thread_recv));
    }

    println!("[PictoCrab] Waiting for connection");
    for stream in listener.incoming() {
        let Ok(stream) = stream else {continue};
        let process_id = stream.client_process_id().unwrap();
        println!("[PictoCrab] Connected to process {:?}", process_id);
        if let Err(e) = read_loop(stream, cached_images.clone(), &thread_channels) {
            println!("[PictoCrab] Error with client {:?}: {:?}", process_id, e)
        }
    }
}
