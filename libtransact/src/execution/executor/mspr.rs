/*
 * Copyright 2019 Bitwise IO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * -----------------------------------------------------------------------------
 */

//! Multi-send, Priority Receive chanels

use std::cell::RefCell;
use std::cmp::Ord;
use std::collections::BinaryHeap;
use std::sync::mpsc::{channel, Receiver, RecvError, TryRecvError};

pub use std::sync::mpsc::Sender;

/// Priority Receiver
///
/// A Priority Receiver returns values in a priority defined by the Ord for the received type.
pub struct PriorityReceiver<T>
where
    T: Ord,
{
    internal: Receiver<T>,
    received: RefCell<BinaryHeap<T>>,
}

impl<T> PriorityReceiver<T>
where
    T: Ord,
{
    fn new(internal: Receiver<T>) -> Self {
        Self {
            internal,
            received: RefCell::new(BinaryHeap::new()),
        }
    }

    pub fn recv(&self) -> Result<T, RecvError> {
        let mut heap = self.received.borrow_mut();
        heap.extend(self.internal.try_iter());

        if !heap.is_empty() {
            return Ok(heap.pop().unwrap());
        }

        self.internal.recv()
    }

    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        let mut heap = self.received.borrow_mut();
        heap.extend(self.internal.try_iter());

        if !heap.is_empty() {
            return Ok(heap.pop().unwrap());
        }

        self.internal.try_recv()
    }

    pub fn iter(&self) -> Iter<T> {
        Iter { rx: self }
    }

    pub fn try_iter(&self) -> TryIter<T> {
        TryIter { rx: self }
    }
}

pub struct Iter<'a, T>
where
    T: Ord + 'a,
{
    rx: &'a PriorityReceiver<T>,
}

impl<'a, T> Iterator for Iter<'a, T>
where
    T: Ord + 'a,
{
    type Item = T;

    fn next(&mut self) -> Option<T> {
        self.rx.recv().ok()
    }
}

pub struct TryIter<'a, T>
where
    T: Ord + 'a,
{
    rx: &'a PriorityReceiver<T>,
}

impl<'a, T> Iterator for TryIter<'a, T>
where
    T: Ord + 'a,
{
    type Item = T;

    fn next(&mut self) -> Option<T> {
        self.rx.try_recv().ok()
    }
}

pub fn priority_channel<T>() -> (Sender<T>, PriorityReceiver<T>)
where
    T: Ord,
{
    let (sender, internal) = channel();
    (sender, PriorityReceiver::new(internal))
}

#[cfg(test)]
mod test {
    use super::*;

    use std::cmp::{Ord, Ordering, PartialOrd};

    #[test]
    fn priority_recv() {
        let (tx, rx) = priority_channel();

        tx.send(1).unwrap();
        tx.send(5).unwrap();
        tx.send(2).unwrap();

        assert_eq!(Ok(5), rx.recv());
        assert_eq!(Ok(2), rx.recv());

        tx.send(6).unwrap();

        assert_eq!(Ok(6), rx.recv());
        assert_eq!(Ok(1), rx.recv());
    }

    #[test]
    fn priority_iter() {
        let rx = {
            let (tx, rx) = priority_channel();

            tx.send(1).unwrap();
            tx.send(10).unwrap();
            tx.send(100).unwrap();

            rx
        };

        assert_eq!(vec![100, 10, 1], rx.iter().collect::<Vec<_>>());
    }

    #[test]
    fn priority_enum() {
        let (tx, rx) = priority_channel();

        tx.send(Event::Byte(1)).unwrap();
        tx.send(Event::Short(5)).unwrap();
        tx.send(Event::Int(2)).unwrap();

        assert_eq!(Ok(Event::Byte(1)), rx.recv());
        assert_eq!(Ok(Event::Short(5)), rx.recv());

        tx.send(Event::Stop).unwrap();

        assert_eq!(Ok(Event::Stop), rx.recv());
        assert_eq!(Ok(Event::Int(2)), rx.recv());
    }

    #[derive(Debug, PartialEq, Eq)]
    enum Event {
        Byte(i8),
        Short(i16),
        Int(i32),
        Stop,
    }

    impl PartialOrd for Event {
        fn partial_cmp(&self, other: &Event) -> Option<Ordering> {
            Some(self.cmp(other))
        }
    }

    impl Ord for Event {
        fn cmp(&self, other: &Event) -> Ordering {
            if self == &Event::Stop {
                Ordering::Greater
            } else if other == &Event::Stop {
                Ordering::Less
            } else {
                Ordering::Equal
            }
        }
    }
}
