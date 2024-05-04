use std::borrow::Borrow;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;

#[derive(Debug)]
pub struct SizeHashMap<K, V> {
    capacity: usize,
    key_vec:  Vec<K>,
    hmap:     HashMap<K, V>,
}

impl<K, V> SizeHashMap<K, V>
where
    K: Eq + Hash + Debug,
    K: Clone,
    V: Debug,
{
    pub fn with_capacity(capacity: usize) -> SizeHashMap<K, V> {
        SizeHashMap {
            capacity,
            key_vec: Vec::with_capacity(capacity),
            hmap: HashMap::with_capacity(capacity),
        }
    }

    // #[deprecated]
    pub fn size(&self) -> usize {
        self.hmap.len()
    }

    pub fn keys(&self) -> &Vec<K> {
        &self.key_vec
    }

    /// 只在Key不存在时新添加内容, Key会添加到最后一个
    pub fn get_or_insert_with<'a, 'b, F>(&'a mut self, k: K, f: F) -> &'b mut V
    where
        F: FnOnce() -> V,
        'a: 'b,
    {
        if self.hmap.contains_key(&k) {
            return self.hmap.get_mut(&k).unwrap();
        }
        if self.hmap.len() >= self.capacity {
            let rk = self.key_vec.remove(0);
            self.hmap.remove(&rk);
        }
        self.hmap.entry(k).or_insert_with_key(|key| {
            self.key_vec.push(key.clone());
            f()
        })
    }

    pub fn get_or_insert_with_key<'a, 'b, F>(&'a mut self, k: K, f: F) -> &'b mut V
    where
        F: FnOnce(&K) -> V,
        'a: 'b,
    {
        if self.hmap.contains_key(&k) {
            return self.hmap.get_mut(&k).unwrap();
        }
        if self.hmap.len() >= self.capacity {
            let rk = self.key_vec.remove(0);
            self.hmap.remove(&rk);
        }
        self.hmap.entry(k).or_insert_with_key(|key| {
            self.key_vec.push(key.clone());
            f(key)
        })
    }

    // 添加或替换内容, 并改变Key的顺序
    pub fn insert(&mut self, k: K, v: V) {
        let Self {
            ref mut hmap,
            ref mut key_vec,
            ..
        } = self;

        hmap.insert(k.clone(), v);

        let k_idx = key_vec.iter().position(|v| v == &k);
        if let Some(k_idx) = k_idx {
            key_vec.remove(k_idx);
        } else if key_vec.len() >= self.capacity {
            let remove_key = key_vec.remove(0);
            hmap.remove(&remove_key);
        }
        key_vec.push(k)
    }

    pub fn entry(&mut self, k: K) -> Entry<K, V> {
        self.hmap.entry(k)
    }

    pub fn get<Q>(&self, k: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.hmap.get(k)
    }

    pub fn get_mut<Q>(&mut self, k: &Q) -> Option<&mut V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.hmap.get_mut(k)
    }

    pub fn last(&self) -> Option<&V> {
        self.key_vec.last().and_then(|k| self.hmap.get(k))
    }

    // pub fn last_mut(&mut self) -> Option<&mut V> {
    //     self.key_vec.last().and_then(|k| self.hmap.get_mut(k))
    // }
}

#[cfg(test)]
mod tests {

    use std::collections::hash_map::Entry;

    use super::SizeHashMap;

    #[test]
    fn test() {
        let mut size_hmap = SizeHashMap::with_capacity(3);

        for i in 0..=10 {
            println!("-------------");
            let num = size_hmap.get_or_insert_with(i, || i);
            println!("current:{}: {}", i, num);
            // *num *= 100;
            // let num = *num;
            size_hmap.size();
            // println!("after {:?}", size_hmap.get_or_insert_with(i, || i * 1000));
            // println!("#{},{}", size_hmap.hmap.len(), size_hmap.key_vec.len());
            // let num2 = size_hmap.get_or_insert_with(i, || i * 1000);
            // assert_eq!(*num2, i * 100);
            // assert_eq!(num, *num2);
            println!("1: {:?}", size_hmap.hmap);
            println!("1: {:?}", size_hmap.key_vec);
        }

        size_hmap.get_or_insert_with(9, || 200);
        println!("2: {:?}", size_hmap.hmap);
        println!("2: {:?}", size_hmap.key_vec);
    }

    #[test]
    fn test_add() {
        let mut size_hmap = SizeHashMap::with_capacity(3);

        for i in 0..=10 {
            size_hmap.insert(i, i);
            println!("{:?} {:?}", size_hmap.size(), size_hmap);
        }
        size_hmap.insert(8, 80);
        println!("{:?} {:?}", size_hmap.size(), size_hmap);
    }

    #[test]
    fn test_entry() {
        #[derive(Debug)]
        struct Tmp {
            tmp: i32,
        }

        let mut hmap = SizeHashMap::with_capacity(3);
        hmap.insert("1", Tmp { tmp: 100 });
        println!("1:{:?}", hmap);
        let entry = hmap.entry("1");
        if let Entry::Occupied(mut tmp) = entry {
            let tmp = tmp.get_mut();
            tmp.tmp = 20000
        } else {
            entry.or_insert(Tmp { tmp: 200 });
        }
        println!("2:{:?}", hmap);
        let tmp = hmap.get(&"1").unwrap();
        println!("3:{:?}", tmp);
    }

    #[test]
    fn test_print_key() {
        let mut map = SizeHashMap::with_capacity(3);
        map.insert(1, 1);
        map.insert(2, 1);
        map.insert(3, 1);

        let str = map
            .key_vec
            .iter()
            .map(|v| format!("{}", v))
            .collect::<Vec<String>>()
            .join(",");
        println!("{}, {:?}", str, map.key_vec);
    }
}
