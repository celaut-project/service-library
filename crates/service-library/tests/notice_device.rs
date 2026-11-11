//! The notice read from a real tty, as the guest reads `/dev/hvc0` (nodo#645).
//!
//! The device of the notice is a tty, in canonical mode, and the node writes to the other side
//! of it. A pty pair of the host is the same thing: the test writes to the master, and the
//! library reads the slave, which it opens once.

use std::ffi::CStr;
use std::fs::File;
use std::io::Write;
use std::os::fd::{FromRawFd, OwnedFd};
use std::time::Duration;

use service_library::notice::{watch_device, Notice, Verb};

/// A pty pair: the master as a file, and the path of the slave.
fn pty() -> (File, String) {
    // SAFETY: plain calls of libc on a descriptor that this function owns.
    unsafe {
        let master = libc::posix_openpt(libc::O_RDWR | libc::O_NOCTTY);
        assert!(master >= 0, "posix_openpt");
        assert_eq!(libc::grantpt(master), 0);
        assert_eq!(libc::unlockpt(master), 0);
        let mut name = [0 as libc::c_char; 128];
        assert_eq!(libc::ptsname_r(master, name.as_mut_ptr(), name.len()), 0);
        let path = CStr::from_ptr(name.as_ptr()).to_str().unwrap().to_string();
        (File::from(OwnedFd::from_raw_fd(master)), path)
    }
}

#[tokio::test]
async fn the_lines_of_the_node_come_through_a_tty_with_one_open() {
    let (mut master, slave_path) = pty();
    let mut notices = watch_device(&slave_path).await.unwrap();

    // What the node writes. The tty is in canonical mode, so a read gives one line each.
    master
        .write_all(b"STOP 1794399680\n")
        .expect("the write to the master");
    let first = tokio::time::timeout(Duration::from_secs(5), notices.next())
        .await
        .expect("the first line arrives")
        .unwrap();
    assert_eq!(
        first,
        Notice {
            verb: Verb::Stop,
            deadline: 1794399680
        }
    );

    // A second and a third line arrive on the same open of the slave: the library opens the
    // device once.
    master
        .write_all(b"HIBERNATE 1794399700\n")
        .expect("the write to the master");
    master
        .write_all(b"STOP 1794399720\n")
        .expect("the write to the master");
    let second = tokio::time::timeout(Duration::from_secs(5), notices.next())
        .await
        .expect("the second line arrives")
        .unwrap();
    assert_eq!(second.verb, Verb::Hibernate);
    let third = tokio::time::timeout(Duration::from_secs(5), notices.next())
        .await
        .expect("the third line arrives")
        .unwrap();
    assert_eq!(third.deadline, 1794399720);
}

#[tokio::test]
async fn the_echo_of_the_tty_and_a_line_with_carriage_return_are_not_a_fault() {
    let (mut master, slave_path) = pty();
    let mut notices = watch_device(&slave_path).await.unwrap();
    master
        .write_all(b"STOP 77\r\n")
        .expect("the write to the master");
    let notice = tokio::time::timeout(Duration::from_secs(5), notices.next())
        .await
        .expect("the line arrives")
        .unwrap();
    assert_eq!(notice.deadline, 77);
}

/// The process of a service ends while the device is open and silent. A reader that waits in a
/// thread for the next line never ends, and the runtime that holds it never closes: the VM
/// would stay up after the service had finished.
#[test]
fn the_runtime_ends_while_the_device_is_open_and_silent() {
    let (_master, slave_path) = pty();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let _notices = watch_device(&slave_path).await.unwrap();
            tokio::time::sleep(Duration::from_millis(100)).await; // the reader is waiting
        });
        drop(runtime);
        let _ = done_tx.send(());
    });
    done_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("the runtime ended with the device open and no line");
}
