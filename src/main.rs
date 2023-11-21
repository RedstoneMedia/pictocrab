use std::io::{Read, Write};
use std::collections::{HashMap, HashSet};
use std::ffi::OsStr;
use std::sync::{Arc, RwLock, mpsc};
use anyhow::anyhow;
use interprocess::os::windows::named_pipe::{PipeListener, DuplexBytePipeStream, PipeListenerOptions, PipeMode};
use image::{ImageFormat, GenericImageView, EncodableLayout};
use sysinfo::{System, SystemExt, RefreshKind};
use once_cell::sync::OnceCell;
use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

const GETS_THREAD_COUNT: usize = 12;
const BUFFER_SIZE: usize = 4096;
const PIPE_NAME: &str = "img_process_server";
const MIN_AVAILABLE_MEMORY : u64 = 2;

static CACHE_DIR : OnceCell<String> = OnceCell::new();
static THREADED_READS: OnceCell<bool> = OnceCell::new();

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
    let sys = System::new_with_specifics(RefreshKind::with_memory(Default::default()));
    let available_memory = sys.available_memory();
    let mut unlocked_cache = cached_images.write().expect("Cannot write to cache");

    if (available_memory / 1000000000) < MIN_AVAILABLE_MEMORY {
        let cache_id = unlocked_cache.0.len() as u32;
        std::fs::write(get_disk_cache_path(&cache_id)?, img_bytes.as_bytes())?;
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
        match reqwest::blocking::get(path) {
            Ok(res) => {
                match res.error_for_status() {
                    Ok(response) => {
                       response.bytes().unwrap().to_vec()
                    }
                    Err(err) => {
                        return Err(anyhow!("Error getting : {}", err));
                    }
                }
            }
            Err(err) => {
                return Err(anyhow!("Error with path {} getting : {}", path, err));
            }
        }
    } else {
        if !*THREADED_READS.get().ok_or(anyhow!("Not setup"))? {
            // Just get the write guard first, which will prevent any other threads from reading images at the same time
            // This can improve performance, if reading off hard drives, because the seek head then doesn't have to move as much
            let _guard = cached_images.write().expect("Could not get write lock");
            std::fs::read(path)?
            // guard gets dropped here
        } else {
            std::fs::read(path)?
        }
    };
    #[cfg(feature = "log")]
    println!("r: {}ns", instant.elapsed().as_nanos());
    #[cfg(feature = "log")]
    let instant = std::time::Instant::now();

    let mut img = image::load_from_memory(&raw_img_bytes)?;
    if img.width() != width || img.height() != height {
        img = img.thumbnail_exact(width, height);
    }
    let mut bmp_img_bytes = Vec::new();
    img.write_to(&mut bmp_img_bytes, ImageFormat::Bmp)?;
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

    let mut thread_chunks : Vec<_> = paths.chunks(paths.len() / GETS_THREAD_COUNT).map(|v| Vec::from(v)).collect();
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


fn setup(disk_cache_dir: &str, working_dir: &str, threaded_reads: bool) -> anyhow::Result<()> {
    std::env::set_current_dir(working_dir)?;
    if CACHE_DIR.get().is_some() {return Ok(());}
    CACHE_DIR.set(disk_cache_dir.to_string()).expect("Can only setup once!");
    THREADED_READS.set(threaded_reads).unwrap();
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

fn process_command(args : Vec<&str>, stream : &mut DuplexBytePipeStream, cached_images : &CachedImageShared, thread_channels: &ThreadChannels) -> anyhow::Result<()> {
    match args[0] {
        "clear_cache" => clear_cache(cached_images)?,
        "setup" => setup(args[1], args[2], args[3] == "true")?,
        "gets" => gets_images(stream, cached_images, thread_channels, args[1].parse::<u32>().unwrap(), args[2].parse::<u32>().unwrap(), &args[3..])?,
        "get" => {get_image(stream, cached_images, args[1],  args[2].parse::<u32>().unwrap(), args[3].parse::<u32>().unwrap())?},
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
    process_command(args, stream, cached_images, thread_channels)?;
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

    println!("[ImgProcessServer] Waiting for connection");
    for stream in listener.incoming() {
        let Ok(stream) = stream else {continue};
        let process_id = stream.client_process_id().unwrap();
        println!("[ImgProcessServer] Connected to process {:?}", process_id);
        if let Err(e) = read_loop(stream, cached_images.clone(), &thread_channels) {
            println!("[ImgProcessServer] Error with client {:?}: {:?}", process_id, e)
        }
    }
}
