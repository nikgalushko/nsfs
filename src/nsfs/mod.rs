mod error;

use crate::nsfs;
use crate::nsfs::error::Error;

use fuser::{FileAttr, FileType};
use std::collections::HashMap;
use std::ffi::{OsStr, OsString};
use std::time::SystemTime;

pub(crate) struct Node {
    pub(crate) index: INode,
    pub(crate) parent: INode,
    pub(crate) name: OsString,
    pub(crate) kind: FileType,
    pub(crate) children: HashMap<OsString, Node>,
}

impl Node {
    pub(crate) fn new_directory(index: INode, parent: INode, name: &OsStr) -> Self {
        Self {
            index,
            parent,
            name: name.to_os_string(),
            children: Default::default(),
            kind: FileType::Directory,
        }
    }

    fn new_file(index: INode, parent: INode, name: &OsStr) -> Self {
        Self {
            index,
            parent,
            name: name.to_os_string(),
            children: Default::default(),
            kind: FileType::RegularFile,
        }
    }
}

struct File {
    data: Vec<u8>,
}

impl File {
    fn new() -> Self {
        Self { data: Vec::new() }
    }
}

type FileDescriptor = u64;
type INode = u64;
pub(crate) struct NsFS {
    pub(crate) attrs: HashMap<INode, FileAttr>,
    pub(crate) nodes: HashMap<INode, Node>,
    pub(crate) open_files: HashMap<FileDescriptor, INode>,
    files: HashMap<INode, File>,
    current_inode: u64,
    current_file_descriptor: FileDescriptor,
}

impl NsFS {
    pub(crate) fn new() -> Self {
        let root = Node {
            index: 1,
            parent: 0,
            name: OsString::from("/"),
            children: Default::default(),
            kind: FileType::Directory,
        };

        let now = SystemTime::now();
        let mut attrs: HashMap<u64, FileAttr> = Default::default();
        attrs.insert(
            1,
            FileAttr {
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
            },
        );

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

    pub(crate) fn next_inode(&mut self) -> u64 {
        self.current_inode += 1;
        self.current_inode
    }

    pub(crate) fn open_file(&mut self, ino: INode) -> FileDescriptor {
        let fd = self.current_file_descriptor;
        self.current_file_descriptor += 1;
        self.open_files.insert(fd, ino);
        fd
    }

    pub(crate) fn find_node(&self, parent: INode, name: &OsStr) -> Result<&Node, Error> {
        let parent = match self.nodes.get(&parent) {
            Some(node) => node,
            None => return Err(Error::NotFound),
        };

        let node = match parent.children.get(name) {
            Some(node) => node,
            None => return Err(Error::NotFound),
        };

        return Ok(node);
    }

    pub(crate) fn get_attr(&self, ino: INode) -> Result<&FileAttr, Error> {
        match self.attrs.get(&ino) {
            Some(attrs) => Ok(attrs),
            None => Err(Error::AttrsNotFound),
        }
    }

    pub(crate) fn read_file(
        &mut self,
        ino: INode,
        size: usize,
        offset: usize,
    ) -> Result<&[u8], Error> {
        let file = match self.files.get(&ino) {
            Some(file) => file,
            None => return Err(Error::FileNotFound),
        };

        let attrs = match self.attrs.get_mut(&ino) {
            Some(attrs) => attrs,
            None => return Err(Error::AttrsNotFound),
        };
        attrs.atime = SystemTime::now();

        let mut size = size as usize;
        let offset = offset as usize;

        if offset >= file.data.len() {
            return Err(Error::EOF);
        }

        if offset + size >= file.data.len() {
            size = file.data.len() - offset; // TODO: а может и не нужно??
        }

        Ok(&file.data[offset..offset + size])
    }

    pub(crate) fn write_file(
        &mut self,
        ino: INode,
        data: &[u8],
        offset: usize,
    ) -> Result<usize, Error> {
        let file = match self.files.get_mut(&ino) {
            Some(file) => file,
            None => return Err(Error::FileNotFound),
        };

        let attrs = match self.attrs.get_mut(&ino) {
            Some(attrs) => attrs,
            None => return Err(Error::AttrsNotFound),
        };

        let offset: usize = offset as usize;

        if offset >= data.len() {
            // extend with zeroes until we are at least at offset
            file.data
                .extend(std::iter::repeat(0).take(offset - file.data.len()));
        }

        if offset + data.len() > file.data.len() {
            file.data.splice(offset.., data.iter().cloned());
        } else {
            file.data
                .splice(offset..offset + data.len(), data.iter().cloned());
        }

        let now = SystemTime::now();
        attrs.atime = now;
        attrs.mtime = now;
        attrs.size = file.data.len() as u64;

        Ok(data.len())
    }

    pub(crate) fn create_file(
        &mut self,
        parent: INode,
        name: &OsStr,
        flags: u32,
    ) -> Result<(&FileAttr, FileDescriptor), Error> {
        let ino = self.next_inode();
        let parent_node = match self.nodes.get_mut(&parent) {
            Some(node) => node,
            None => return Err(Error::NotFound),
        };

        if parent_node.children.contains_key(name) {
            return Err(Error::AlreadyExists);
        }

        let ts = SystemTime::now();
        self.attrs.insert(
            ino,
            FileAttr {
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
            },
        );
        self.files.insert(ino, File::new());

        let key = name.to_os_string();
        parent_node
            .children
            .entry(key)
            .or_insert(Node::new_file(ino, parent, name));

        let fh = self.open_file(ino);
        Ok((self.attrs.get(&ino).unwrap(), fh))
    }
}
