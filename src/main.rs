use std::collections::HashMap;
use std::error::Error;

use fuser::{TimeOrNow, Filesystem, Request, ReplyEntry, ReplyAttr, ReplyData, ReplyEmpty, ReplyOpen, ReplyWrite, ReplyStatfs, ReplyXattr, ReplyCreate, ReplyBmap, ReplyLock, ReplyDirectory, FileAttr, FileType};

use std::time::{Duration, SystemTime};
use std::ffi::{OsStr, OsString};
use std::path::{Path};
use std::env;
use libc::{c_int, ENOSYS, ENOENT, EOF, EEXIST};

const TTL: Duration = Duration::from_secs(1);
static CURRENT_DIR:&'static str = ".";
static PARENT_DIR:&'static str = "..";

#[derive(Debug)]
enum MyError {
    NotFound,
    FileNotFound,
    AttrsNotFound,
    EOF,
    AlreadyExists,
}

impl Error for MyError {}

impl From<MyError> for c_int {
    fn from(value: MyError) -> Self {
        match value {
            MyError::NotFound | MyError::AttrsNotFound | MyError::FileNotFound => ENOENT,
            MyError::EOF => EOF,
            MyError::AlreadyExists => EEXIST,
        }
    }
}

impl std::fmt::Display for MyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MyError::NotFound => write!(f, "not found"),
            MyError::AttrsNotFound => write!(f, "attributes not found"),
            MyError::FileNotFound => write!(f, "file not found"),
            MyError::EOF => write!(f, "eof"),
            MyError::AlreadyExists => write!(f, "already exists")
        }
    }
}

struct Node {
    index: INode,
    parent: INode,
    name: OsString,
    kind: FileType,
    children: HashMap<OsString, Node>
}

impl Node {
    fn new_directory(index: INode, parent: INode, name: &OsStr) -> Self{
        Self{
            index,
            parent,
            name: name.to_os_string(),
            children: Default::default(),
            kind: FileType::Directory,
        }
    }

    fn new_file(index: INode, parent: INode, name: &OsStr) -> Self{
        Self{
            index,
            parent,
            name: name.to_os_string(),
            children: Default::default(),
            kind: FileType::RegularFile,
        }
    }
}

struct File {
    data: Vec<u8>
}

impl File {
    fn new() -> Self {
        Self{ data: Vec::new() }
    }
}

type FileDescriptor = u64;
type INode = u64;

struct NsFS {
    attrs: HashMap<INode, FileAttr>,
    nodes: HashMap<INode, Node>,
    open_files: HashMap<FileDescriptor, INode>,
    files: HashMap<INode, File>,
    current_inode: u64,
    current_file_descriptor: FileDescriptor,
}

impl NsFS {
    fn new() -> Self {
        let root = Node{
            index: 1,
            parent: 0,
            name: OsString::from("/"),
            children: Default::default(),
            kind: FileType::Directory,
        };

        let now = SystemTime::now();
        let mut attrs: HashMap<u64, FileAttr> = Default::default();
        attrs.insert(1, FileAttr{
            ino: 1,
            size: 0,
            blocks: 0,
            atime: now,
            mtime: now,
            ctime: now,
            crtime: now,
            kind: FileType::Directory,
            perm: 0,
            nlink: 0,
            uid: 0,
            gid: 0,
            rdev: 0,
            blksize: 0,
            flags: 0,
        });

        let mut nodes: HashMap<u64, Node> = Default::default();
        nodes.insert(1, root);

        Self {
            attrs,
            nodes,
            current_inode: 1, // 1 is root TODO: add root to attrs
            open_files: Default::default(),
            files: Default::default(),
            current_file_descriptor: 0,
        }
    }

    fn next_indoe(&mut self) -> u64 {
        self.current_inode += 1;
        self.current_inode
    }

    fn open_file(&mut self, ino: INode) -> FileDescriptor {
        let fd = self.current_file_descriptor;
        self.current_file_descriptor += 1;
        self.open_files.insert(fd, ino);
        fd
    }

    fn find_node(&self, parent: INode, name: &OsStr) -> Result<&Node, MyError> {
        let parent = match self.nodes.get(&parent) {
            Some(node) => node,
            None => return Err(MyError::NotFound)
        };

        let node = match parent.children.get(name) {
            Some(node) => node,
            None => return Err(MyError::NotFound)
        };

        return Ok(node);
    }

    fn get_attr(&self, ino: INode) -> Result<&FileAttr, MyError> {
        match self.attrs.get(&ino) {
            Some(attrs) => Ok(attrs),
            None => Err(MyError::NotFound)
        }
    }

    fn read_file(&mut self, ino: INode, size: usize, offset: usize) -> Result<&[u8], MyError> {
        let file = match self.files.get(&ino) {
            Some(file) => file,
            None => {
                return Err(MyError::NotFound)
            }
        };

        let attrs = match self.attrs.get_mut(&ino) {
            Some(attrs) => attrs,
            None => {
                return Err(MyError::NotFound)
            }
        };
        attrs.atime = SystemTime::now();

        let mut size = size as usize;
        let offset = offset as usize;

        if offset >= file.data.len() {
            return Err(MyError::EOF)
        }

        if offset + size >= file.data.len() {
            size = file.data.len() - offset; // TODO: а может и не нужно??
        }

        Ok(&file.data[offset..offset+size])
    }

    fn write_file(&mut self, ino: INode, data: &[u8], offset: usize) -> Result<usize, MyError> {
        let file = match self.files.get_mut(&ino) {
            Some(file) => file,
            None => {
                return Err(MyError::NotFound)
            }
        };

        let attrs = match self.attrs.get_mut(&ino) {
            Some(attrs) => attrs,
            None => {
                return Err(MyError::NotFound)
            }
        };
        

        let offset: usize = offset as usize;

        if offset >= data.len() {
            // extend with zeroes until we are at least at offset
            file.data.extend(std::iter::repeat(0).take(offset - file.data.len()));
        }

        if offset + data.len() > file.data.len() {
            file.data.splice(offset.., data.iter().cloned());
        } else {
            file.data.splice(offset..offset + data.len(), data.iter().cloned());
        }

        let now = SystemTime::now();
        attrs.atime = now;
        attrs.mtime = now;
        attrs.size = file.data.len() as u64;


        Ok(data.len())
    }

    fn create_file(&mut self, parent: INode, name: &OsStr, flags: u32) -> Result<(&FileAttr, FileDescriptor), MyError> {
        let ino = self.next_indoe();
        let parent_node = match self.nodes.get_mut(&parent) {
            Some(node) => node,
            None => {
                return Err(MyError::NotFound)
            }
        };

        if parent_node.children.contains_key(name) {
            return Err(MyError::AlreadyExists)
        }

        let ts = SystemTime::now();
        self.attrs.insert(ino, FileAttr{
            ino,
            size: 0,
            blocks: 0,
            atime: ts,
            mtime: ts,
            ctime: ts,
            crtime: ts,
            kind: FileType::RegularFile,
            perm: 0o777,
            nlink: 0,
            uid: 0,
            gid: 0,
            rdev: 0,
            blksize: 0,
            flags,
        });
        self.files.insert(ino, File::new());

        let key = name.to_os_string();
        parent_node.children.entry(key).or_insert(Node::new_file(ino, parent, name));


        let fh = self.open_file(ino);
        Ok((self.attrs.get(&ino).unwrap(), fh))
    }
}

impl Filesystem for NsFS {
    /// Look up a directory entry by name and get its attributes.
    fn lookup(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let node = match self.find_node(parent, name) {
            Ok(node) => node,
            Err(err) => {
                reply.error(c_int::from(err));
                return
            }
        };

        let attrs = self.attrs.get(&node.index).unwrap();
        reply.entry(&TTL, attrs, 0);
    }

    /// Forget about an inode.
    /// The nlookup parameter indicates the number of lookups previously performed on
    /// this inode. If the filesystem implements inode lifetimes, it is recommended that
    /// inodes acquire a single reference on each lookup, and lose nlookup references on
    /// each forget. The filesystem may ignore forget calls, if the inodes don't need to
    /// have a limited lifetime. On unmount it is not guaranteed, that all referenced
    /// inodes will receive a forget message.
    fn forget(&mut self, _req: &Request, _ino: u64, _nlookup: u64) {}

    /// Get file attributes.
    fn getattr(&mut self, _req: &Request, ino: u64, reply: ReplyAttr) {
        match self.get_attr(ino) {
            Ok(attrs) => {
                reply.attr(&TTL, attrs);
            },
            Err(err) => reply.error(c_int::from(err))
        }
    }

    /// Set file attributes.
    fn setattr(&mut self, _req: &Request<'_>, ino: u64, _mode: Option<u32>, uid: Option<u32>, gid: Option<u32>, size: Option<u64>, atime: Option<TimeOrNow>, mtime: Option<TimeOrNow>, ctime: Option<SystemTime>, _fh: Option<u64>, crtime: Option<SystemTime>, _chgtime: Option<SystemTime>, _bkuptime: Option<SystemTime>, _flags: Option<u32>, reply: ReplyAttr) {
        let mut file = match self.attrs.get_mut(&ino) {
            Some(file) => file,
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        if let Some(uid) = uid { file.uid = uid; }
        if let Some(gid) = gid { file.gid = gid; }
        if let Some(size) = size { file.size = size; }
        if let Some(atime) = atime {
            match atime {
            TimeOrNow::Now => file.atime = SystemTime::now(),
            TimeOrNow::SpecificTime(specific_time) => file.atime = specific_time,
            }

        }
        if let Some(mtime) = mtime {
            match mtime {
            TimeOrNow::Now => file.mtime = SystemTime::now(),
            TimeOrNow::SpecificTime(specific_time) => file.mtime = specific_time,
            }

        }
        if let Some(crtime) = crtime { file.crtime = crtime; }

        reply.attr(&TTL, file);
    }

    /// Read symbolic link.
    fn readlink(&mut self, _req: &Request<'_>, _ino: u64, reply: ReplyData) {
        reply.error(ENOSYS);
    }

    /// Create file node.
    /// Create a regular file, character device, block device, fifo or socket node.
    fn mknod(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, _mode: u32, _umas: u32, _rdev: u32, reply: ReplyEntry) {
        println!("mknod; parent: {}, name: {:?}", parent, name);
        reply.error(ENOSYS);
    }

    /// Create a directory.
    fn mkdir(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, _mode: u32, _umask: u32, reply: ReplyEntry) {
        let ino = self.next_indoe();

        let parent_node = match self.nodes.get_mut(&parent) {
            Some(node) => node,
            None => {
                reply.error(ENOENT);
                return;
            }
        };
        if parent_node.children.contains_key(name) {
            reply.error(EEXIST);
            return;
        }

        let ts = SystemTime::now();
        self.attrs.insert(ino, FileAttr{
            ino: ino,
            size: 0,
            blocks: 0,
            atime: ts,
            mtime: ts,
            ctime: ts,
            crtime: ts,
            kind: FileType::Directory,
            perm: 0o777,
            nlink: 0,
            uid: 0,
            gid: 0,
            rdev: 0,
            blksize: 0,
            flags: 0,
        });

        let key = name.to_os_string();
        parent_node.children.insert(key, Node::new_directory(ino, parent, name));

        reply.entry(&TTL, self.attrs.get(&ino).unwrap(), 0);
    }

    /// Remove a file.
    fn unlink(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        println!("unlink start; parent: {}, name: {:?}", parent, name);
        let parent_node = match self.nodes.get_mut(&parent) {
            None => {
                reply.error(ENOENT);
                return;
            },
            Some(parent) => parent,
        };

        let victim = match parent_node.children.remove(name) {
            None => {
                reply.error(ENOENT);
                return;
            },
            Some(victim) => victim,
        };

        self.attrs.remove(&victim.index);
        self.nodes.remove(&victim.index);
        self.open_files.remove(&victim.index);

        println!("unlink end; parent: {}, name: {:?}", parent, name);
        reply.ok();
    }

    /// Remove a directory.
    fn rmdir(&mut self, _req: &Request<'_>, _parent: u64, _name: &OsStr, reply: ReplyEmpty) {
        reply.error(ENOSYS);
    }

    /// Create a symbolic link.
    fn symlink(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, _link: &Path, reply: ReplyEntry) {
        reply.error(ENOSYS);
    }

    /// Rename a file.
    fn rename(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, _newparent: u64, _newname: &OsStr, _flags: u32, reply: ReplyEmpty) {
        reply.error(ENOSYS);
    }

    /// Create a hard link.
    fn link(&mut self, _req: &Request<'_>, ino: u64, _newparent: u64, newname: &OsStr, reply: ReplyEntry) {
        reply.error(ENOSYS);
    }

    /// Open a file.
    /// Open flags (with the exception of O_CREAT, O_EXCL, O_NOCTTY and O_TRUNC) are
    /// available in flags. Filesystem may store an arbitrary file handle (pointer, index,
    /// etc) in fh, and use this in other all other file operations (read, write, flush,
    /// release, fsync). Filesystem may also implement stateless file I/O and not store
    /// anything in fh. There are also some flags (direct_io, keep_cache) which the
    /// filesystem may set, to change the way the file is opened. See fuse_file_info
    /// structure in <fuse_common.h> for more details.
    fn open(&mut self, _req: &Request<'_>, ino: u64, flags: i32, reply: ReplyOpen) {
        // TODO: parse flags
        let fh = self.open_file(ino);
        reply.opened(fh, flags as u32);
    }

    /// Read data.
    /// Read should send exactly the number of bytes requested except on EOF or error,
    /// otherwise the rest of the data will be substituted with zeroes. An exception to
    /// this is when the file has been opened in 'direct_io' mode, in which case the
    /// return value of the read system call will reflect the return value of this
    /// operation. fh will contain the value set by the open method, or will be undefined
    /// if the open method didn't set any value.
    fn read(&mut self, _req: &Request<'_>, ino: u64, _fh: u64, offset: i64, size: u32, _flags: i32, _lock_owner: Option<u64>, reply: ReplyData) {
        match self.read_file(ino, size as usize, offset as usize) {
            Ok(data) => reply.data(data),
            Err(err) => reply.error(c_int::from(err))
        }
    }

    /// Write data.
    /// Write should return exactly the number of bytes requested except on error. An
    /// exception to this is when the file has been opened in 'direct_io' mode, in
    /// which case the return value of the write system call will reflect the return
    /// value of this operation. fh will contain the value set by the open method, or
    /// will be undefined if the open method didn't set any value.
    fn write(&mut self, _req: &Request<'_>, ino: u64, _fh: u64, offset: i64, data: &[u8], _write_flags: u32, _flags: i32, _lock_owner: Option<u64>, reply: ReplyWrite) {
        match self.write_file(ino, data, offset as usize) {
            Ok(size) => reply.written(size as u32),
            Err(err) => reply.error(c_int::from(err))
        }
    }

    /// Flush method.
    /// This is called on each close() of the opened file. Since file descriptors can
    /// be duplicated (dup, dup2, fork), for one open call there may be many flush
    /// calls. Filesystems shouldn't assume that flush will always be called after some
    /// writes, or that if will be called at all. fh will contain the value set by the
    /// open method, or will be undefined if the open method didn't set any value.
    /// NOTE: the name of the method is misleading, since (unlike fsync) the filesystem
    /// is not forced to flush pending writes. One reason to flush data, is if the
    /// filesystem wants to return write errors. If the filesystem supports file locking
    /// operations (setlk, getlk) it should remove all locks belonging to 'lock_owner'.
    fn flush(&mut self, _req: &Request<'_>, _ino: u64, _fh: u64, _lock_owner: u64, reply: ReplyEmpty) {
        reply.ok();
    }

    /// Release an open file.
    /// Release is called when there are no more references to an open file: all file
    /// descriptors are closed and all memory mappings are unmapped. For every open
    /// call there will be exactly one release call. The filesystem may reply with an
    /// error, but error values are not returned to close() or munmap() which triggered
    /// the release. fh will contain the value set by the open method, or will be undefined
    /// if the open method didn't set any value. flags will contain the same flags as for
    /// open.
    fn release(&mut self, _req: &Request<'_>, _ino: u64, fh: u64, _flags: i32, _lock_owner: Option<u64>, _flush: bool, reply: ReplyEmpty) {
        self.open_files.remove(&fh);
        reply.ok();
    }

    /// Synchronize file contents.
    /// If the datasync parameter is non-zero, then only the user data should be flushed,
    /// not the meta data.
    fn fsync(&mut self, _req: &Request<'_>, _ino: u64, _fh: u64, _datasync: bool, reply: ReplyEmpty) {
        // TODO: create a queue of buffers opened files; now we write a data immediately
        reply.ok();
    }

    /// Open a directory.
    /// Filesystem may store an arbitrary file handle (pointer, index, etc) in fh, and
    /// use this in other all other directory stream operations (readdir, releasedir,
    /// fsyncdir). Filesystem may also implement stateless directory I/O and not store
    /// anything in fh, though that makes it impossible to implement standard conforming
    /// directory stream operations in case the contents of the directory can change
    /// between opendir and releasedir.
    fn opendir(&mut self, _req: &Request<'_>, _ino: u64, _flags: i32, reply: ReplyOpen) {
        reply.opened(0, 0);
    }

    /// Read directory.
    /// Send a buffer filled using buffer.fill(), with size not exceeding the
    /// requested size. Send an empty buffer on end of stream. fh will contain the
    /// value set by the opendir method, or will be undefined if the opendir method
    /// didn't set any value.
    fn readdir(&mut self, _req: &Request<'_>, ino: u64, _fh: u64, offset: i64, mut reply: ReplyDirectory) {
        let mut ret:Vec<(u64, FileType, &OsStr)> = Vec::new();
        ret.push((ino, FileType::Directory, OsStr::new(CURRENT_DIR)));

        let node = match self.nodes.get(&ino) {
            Some(node) => node,
            None => {
                reply.error(ENOENT);
                return;
            },
        };

        if node.parent != 0 {
            ret.push((node.parent, FileType::Directory, OsStr::new(PARENT_DIR)));
        }
        for node in node.children.values() {
            ret.push((node.index, node.kind, &node.name));
        }

        ret.iter().skip(offset as usize).enumerate().for_each(|(i, entry)| {
            println!("add to reply entry");
            reply.add(entry.0, offset + i as i64 + 1 as i64, entry.1, &entry.2);
        });
        reply.ok();
    }

    /// Release an open directory.
    /// For every opendir call there will be exactly one releasedir call. fh will
    /// contain the value set by the opendir method, or will be undefined if the
    /// opendir method didn't set any value.
    fn releasedir(&mut self, _req: &Request<'_>, _ino: u64, _fh: u64, _flags: i32, reply: ReplyEmpty) {
        reply.ok();
    }

    /// Synchronize directory contents.
    /// If the datasync parameter is set, then only the directory contents should
    /// be flushed, not the meta data. fh will contain the value set by the opendir
    /// method, or will be undefined if the opendir method didn't set any value.
    fn fsyncdir (&mut self, _req: &Request<'_>, _ino: u64, _fh: u64, _datasync: bool, reply: ReplyEmpty) {
        reply.error(ENOSYS);
    }

    /// Get file system statistics.
    fn statfs(&mut self, _req: &Request<'_>, _ino: u64, reply: ReplyStatfs) {
        reply.statfs(0, 0, 0, 0, 0, 512, 255, 0);
    }

    /// Set an extended attribute.
    fn setxattr(&mut self, _req: &Request<'_>, _ino: u64, _name: &OsStr, _value: &[u8], _flags: i32, _position: u32, reply: ReplyEmpty) {
        reply.error(ENOSYS);
    }

    /// Get an extended attribute.
    /// If `size` is 0, the size of the value should be sent with `reply.size()`.
    /// If `size` is not 0, and the value fits, send it with `reply.data()`, or
    /// `reply.error(ERANGE)` if it doesn't.
    fn getxattr(&mut self, _req: &Request<'_>, _ino: u64, _name: &OsStr, _size: u32, reply: ReplyXattr) {
        reply.error(ENOSYS);
    }

    /// List extended attribute names.
    /// If `size` is 0, the size of the value should be sent with `reply.size()`.
    /// If `size` is not 0, and the value fits, send it with `reply.data()`, or
    /// `reply.error(ERANGE)` if it doesn't.
    fn listxattr(&mut self, _req: &Request<'_>, _ino: u64, _size: u32, reply: ReplyXattr) {
        reply.error(ENOSYS);
    }

    /// Remove an extended attribute.
    fn removexattr(&mut self, _req: &Request<'_>, _ino: u64, _name: &OsStr, reply: ReplyEmpty) {
        reply.error(ENOSYS);
    }

    /// Check file access permissions.
    /// This will be called for the access() system call. If the 'default_permissions'
    /// mount option is given, this method is not called. This method is not called
    /// under Linux kernel versions 2.4.x
    fn access(&mut self, _req: &Request<'_>, _ino: u64, _mask: i32, reply: ReplyEmpty) {
        println!("access");
        reply.ok();
    }

    /// Create and open a file.
    /// If the file does not exist, first create it with the specified mode, and then
    /// open it. Open flags (with the exception of O_NOCTTY) are available in flags.
    /// Filesystem may store an arbitrary file handle (pointer, index, etc) in fh,
    /// and use this in other all other file operations (read, write, flush, release,
    /// fsync). There are also some flags (direct_io, keep_cache) which the
    /// filesystem may set, to change the way the file is opened. See fuse_file_info
    /// structure in <fuse_common.h> for more details. If this method is not
    /// implemented or under Linux kernel versions earlier than 2.6.15, the mknod()
    /// and open() methods will be called instead.
    fn create(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, _mode: u32, _umask: u32, flags: i32, reply: ReplyCreate) {
        let flags = flags as u32;
        match self.create_file(parent, name, flags) {
            Ok((attrs, fh)) => reply.created(&TTL, attrs, 0, fh, flags),
            Err(err) => reply.error(c_int::from(err))
        }
    }

    /// Test for a POSIX file lock.
    fn getlk(&mut self, _req: &Request<'_>, _ino: u64, _fh: u64, _lock_owner: u64, _start: u64, _end: u64, _typ: i32, _pid: u32, reply: ReplyLock) {
        println!("getlk");
        reply.error(ENOSYS);
    }

    /// Acquire, modify or release a POSIX file lock.
    /// For POSIX threads (NPTL) there's a 1-1 relation between pid and owner, but
    /// otherwise this is not always the case.  For checking lock ownership,
    /// 'fi->owner' must be used. The l_pid field in 'struct flock' should only be
    /// used to fill in this field in getlk(). Note: if the locking methods are not
    /// implemented, the kernel will still allow file locking to work locally.
    /// Hence these are only interesting for network filesystems and similar.
    fn setlk(&mut self, _req: &Request<'_>, _ino: u64, _fh: u64, _lock_owner: u64, _start: u64, _end: u64, _typ: i32, _pid: u32, _sleep: bool, reply: ReplyEmpty) {
        println!("setlk");
        reply.error(ENOSYS);
    }

    /// Map block index within file to block index within device.
    /// Note: This makes sense only for block device backed filesystems mounted
    /// with the 'blkdev' option
    fn bmap(&mut self, _req: &Request<'_>, _ino: u64, _blocksize: u32, _idx: u64, reply: ReplyBmap) {
        println!("bmap");
        reply.error(ENOSYS);
    }
}

fn main() {
    env_logger::init();
    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        println!("Usage: {} <source> <mountpoint>", args[0]);
        return;
    }

    let mountpoint = &args[1];

    let fs = NsFS::new();
    fuser::mount2(fs, &mountpoint, &[]).unwrap();
}
