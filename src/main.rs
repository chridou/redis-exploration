extern crate redis;
extern crate uuid;
extern crate rand;

use std::time;
use std::env;
use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;
use rand::Rng;

use uuid::Uuid;

use redis::*;

/// Only set a TTL when the value is empty
const SET_TTL_LUA: &'static str = r#"
if redis.call("EXISTS", KEYS[1]) == 1 then
  local payload = redis.call("GET", KEYS[1])
  if payload == "" then
    redis.call("EXPIRE", KEYS[1], ARGV[1])
  end
end
"#;

fn main() {
    // Fails miserably on false input...
    let port: u16 = env::args().nth(1).unwrap().parse().unwrap();
    let url = format!("redis://localhost:{}", port);
    println!("{}", url);
    let client = Client::open(url.as_ref()).unwrap();
    let conn = client.get_connection().unwrap();

    clear(&conn);
    println!("===== A KEY AND A VALUE MAY BOTH BE BINARY =====");
    measure(|| {
        let k: Vec<u8> = vec![0];
        let v: Vec<u8> = vec![1];
        let _: () = conn.set(k.as_slice(), v.as_slice()).unwrap();
        let res: Vec<u8> = conn.get(k.as_slice()).unwrap();
        assert_eq!(res, v);
    });

    clear(&conn);
    println!("===== SET STORES KEYS WITHOUT A VALUE =====");
    measure(|| {
        let v: Vec<u8> = Vec::new();
        let _: () = conn.set("key", v.as_slice()).unwrap();
        let res: Vec<u8> = conn.get("key").unwrap();
        assert_eq!(res, v);
    });

    clear(&conn);
    println!("===== MGET RETURNS VALUES IN THE ORDER THEY WERE QUERIED =====");
    println!("10000 keys 100 times");
    measure(|| {
        let mut rng = rand::OsRng::new().unwrap();

        let mut keys: Vec<_> = (0..10000).map(|_| Uuid::new_v4().as_bytes().clone()).collect();
        for key in &keys {
            let _: () = conn.set(key, key).unwrap();
        }

        let mut last_hash = hash(&keys);
        for _ in 0..100 {
            rng.shuffle(&mut keys);
            let new_hash = hash(&keys);
            assert!(new_hash != last_hash);
            last_hash = new_hash;

            let query_for: Vec<Vec<u8>> = keys.iter()
                .map(|k| k.iter().cloned().collect::<Vec<u8>>())
                .collect();
            let result: Vec<Vec<u8>> = conn.get(query_for.as_slice()).unwrap();

            for (i, value) in result.iter().enumerate() {
                assert_eq!(query_for[i].as_slice(), value.as_slice());
            }
        }
    });

    clear(&conn);
    println!("===== MGET RETURNS VALUES IN THE ORDER THEY WERE QUERIED INCLUDING NONEXISTING \
              KEYS(QUERYING OPTIONS OF VECTORS) =====");
    println!("5000 existing and 5000 non existing Keys 100 times");
    measure(|| {
        let mut rng = rand::OsRng::new().unwrap();

        #[derive(Hash, PartialEq, Clone, Debug)]
        enum Key {
            // exists
            E(Vec<u8>),
            // does not exist
            N(Vec<u8>),
        }

        impl Key {
            pub fn bytes(&self) -> &[u8] {
                match *self {
                    Key::E(ref k) | Key::N(ref k) => k,
                }
            }
        }

        let existing_keys: Vec<_> = (0..5000).map(|_| Uuid::new_v4().as_bytes().clone()).collect();
        let non_existing_keys: Vec<_> =
            (0..5000).map(|_| Uuid::new_v4().as_bytes().clone()).collect();
        for key in &existing_keys {
            let _: () = conn.set(key, key).unwrap();
        }

        let mut all_keys = Vec::new();
        for k in existing_keys {
            let k: Vec<_> = k.iter().cloned().collect();
            all_keys.push(Key::E(k));
        }
        for k in non_existing_keys {
            let k: Vec<_> = k.iter().cloned().collect();
            all_keys.push(Key::N(k));
        }


        let mut last_hash = hash(&all_keys);
        for _ in 0..100 {
            rng.shuffle(&mut all_keys);
            let new_hash = hash(&all_keys);
            assert!(new_hash != last_hash);
            last_hash = new_hash;

            let query_for: Vec<Vec<u8>> = all_keys.iter()
                .map(|k| k.bytes().iter().cloned().collect::<Vec<u8>>())
                .collect();
            let result: Vec<Option<Vec<u8>>> = conn.get(query_for.as_slice()).unwrap();

            for (i, value_opt) in result.iter().enumerate() {
                let must_be = match *value_opt {
                    Some(ref value) => Key::E(value.clone()),
                    None => Key::N(query_for[i].clone()),
                };
                assert_eq!(must_be, all_keys[i]);
            }
        }
    });

    clear(&conn);
    println!("===== MGET RETURNS VALUES IN THE ORDER THEY WERE QUERIED INCLUDING NONEXISTING \
              KEYS(QUERYING VECTORS WHERE DOES NOT EXIST WILL BE AN EMPTY VECTOR) =====");
    println!("Not the correct way to query: https://redis.io/topics/protocol#array-reply");
    println!("5000 existing and 5000 non existing Keys 100 times");
    measure(|| {
        let mut rng = rand::OsRng::new().unwrap();

        #[derive(Hash, PartialEq, Clone, Debug)]
        enum Key {
            // exists
            E(Vec<u8>),
            // does not exist
            N(Vec<u8>),
        }

        impl Key {
            pub fn bytes(&self) -> &[u8] {
                match *self {
                    Key::E(ref k) | Key::N(ref k) => k,
                }
            }
        }

        let existing_keys: Vec<_> = (0..5000).map(|_| Uuid::new_v4().as_bytes().clone()).collect();
        let non_existing_keys: Vec<_> =
            (0..5000).map(|_| Uuid::new_v4().as_bytes().clone()).collect();
        for key in &existing_keys {
            let _: () = conn.set(key, key).unwrap();
        }

        let mut all_keys = Vec::new();
        for k in existing_keys {
            let k: Vec<_> = k.iter().cloned().collect();
            all_keys.push(Key::E(k));
        }
        for k in non_existing_keys {
            let k: Vec<_> = k.iter().cloned().collect();
            all_keys.push(Key::N(k));
        }


        let mut last_hash = hash(&all_keys);
        for _ in 0..100 {
            rng.shuffle(&mut all_keys);
            let new_hash = hash(&all_keys);
            assert!(new_hash != last_hash);
            last_hash = new_hash;

            let query_for: Vec<Vec<u8>> = all_keys.iter()
                .map(|k| k.bytes().iter().cloned().collect::<Vec<u8>>())
                .collect();
            let result: Vec<Vec<u8>> = conn.get(query_for.as_slice()).unwrap();

            for (i, value) in result.iter().enumerate() {
                let must_be = if value.is_empty() {
                    Key::N(query_for[i].clone())
                } else {
                    Key::E(value.clone())
                };
                assert_eq!(must_be, all_keys[i]);
            }
        }
    });

    clear(&conn);
    println!("===== SET DOES OVERRWRITE A VALUE =====");
    measure(|| {
        let k: Vec<u8> = vec![0];
        let v1: Vec<u8> = vec![1];
        let v2: Vec<u8> = vec![2];
        let _: () = conn.set(k.as_slice(), v1.as_slice()).unwrap();
        let res: Vec<u8> = conn.get(k.as_slice()).unwrap();
        assert_eq!(res, v1);
        let _: () = conn.set(k.as_slice(), v2.as_slice()).unwrap();
        let res: Vec<u8> = conn.get(k.as_slice()).unwrap();
        assert_eq!(res, v2);
    });

    clear(&conn);
    println!("===== SETNX DOES NOT OVERRWRITE A VALUE =====");
    measure(|| {
        let k: Vec<u8> = vec![0];
        let v1: Vec<u8> = vec![1];
        let v2: Vec<u8> = vec![2];
        let _: () = conn.set(k.as_slice(), v1.as_slice()).unwrap();
        let res: Vec<u8> = conn.get(k.as_slice()).unwrap();
        assert_eq!(res, v1);
        let _: () = conn.set_nx(k.as_slice(), v2.as_slice()).unwrap();
        let res: Vec<u8> = conn.get(k.as_slice()).unwrap();
        assert_eq!(res, v1);
    });

    clear(&conn);
    println!("===== PIPELINE: SETNX THEN EXPIRE WILL SET A TTL ON AN EXISTING VALUE =====");
    measure(|| {
        let k: Vec<u8> = vec![0];
        let v1: Vec<u8> = vec![1];
        let v2: Vec<u8> = vec![2];
        let _: () = conn.set(k.as_slice(), v1.as_slice()).unwrap();
        let res: Vec<u8> = conn.get(k.as_slice()).unwrap();
        assert_eq!(res, v1);
        let ttl: i64 = redis::cmd("TTL").arg(k.as_slice()).query(&conn).unwrap();
        // No TTL = -1!
        assert_eq!(ttl, -1);
        let mut pipe = redis::pipe();
        pipe.cmd("SETNX").arg(k.as_slice()).arg(v2.as_slice()).ignore();
        pipe.cmd("EXPIRE").arg(k.as_slice()).arg(20).ignore();
        pipe.execute(&conn);
        let res: Vec<u8> = conn.get(k.as_slice()).unwrap();
        let ttl: i64 = redis::cmd("TTL").arg(k.as_slice()).query(&conn).unwrap();
        assert_eq!(res, v1);
        assert!(ttl > 0);
    });

    println!("===== A LUA SCRIPT CAN CONDITIONALLY SET A TTL =====");
    println!("A LUA script is atomic: https://redis.io/commands/eval#atomicity-of-scripts");
    measure(|| {
        let k: Vec<u8> = vec![0];
        let v1: Vec<u8> = vec![1];
        let empty: Vec<u8> = vec![];
        let _: () = conn.set(k.as_slice(), v1.as_slice()).unwrap();
        let res: Vec<u8> = conn.get(k.as_slice()).unwrap();
        assert_eq!(res, v1);
        let ttl: i64 = redis::cmd("TTL").arg(k.as_slice()).query(&conn).unwrap();
        assert_eq!(ttl, -1);
        redis::cmd("EVAL").arg(SET_TTL_LUA).arg(1).arg(k.as_slice()).arg(5).execute(&conn);
        let ttl: i64 = redis::cmd("TTL").arg(k.as_slice()).query(&conn).unwrap();
        assert_eq!(ttl, -1);
        let _: () = conn.set(k.as_slice(), empty.as_slice()).unwrap();
        redis::cmd("EVAL").arg(SET_TTL_LUA).arg(1).arg(k.as_slice()).arg(5).execute(&conn);
        let ttl: i64 = redis::cmd("TTL").arg(k.as_slice()).query(&conn).unwrap();
        let res: Vec<u8> = conn.get(k.as_slice()).unwrap();
        assert_eq!(res, empty);
        println!("The TTL set by LUA is {}", ttl);
        assert!(ttl > 0);
    });

    clear(&conn);

    println!("===== PIPELINE: SET TTL 10000 TIMES WITH SETNX AND EXPIRE =====");
    measure(|| {
        let keys: Vec<_> = (0..10000)
            .map(|_| Uuid::new_v4().as_bytes().iter().cloned().collect::<Vec<_>>())
            .collect();
        let empty: Vec<u8> = vec![];

        let mut pipe = redis::pipe();

        for k in &keys {
            pipe.cmd("SETNX").arg(k.as_slice()).arg(empty.as_slice()).ignore();
            pipe.cmd("EXPIRE").arg(k.as_slice()).arg(20).ignore();
        }

        pipe.execute(&conn);

        let mut pipe = redis::pipe();

        for k in &keys {
            pipe.get(k.as_slice());
            pipe.cmd("TTL").arg(k.as_slice());
        }

        let results: Vec<(Vec<u8>, i64)> = pipe.query(&conn).unwrap();
        for (vv, ttl) in results {
            assert_eq!(vv, empty.clone());
            assert!(ttl > 0);
        }
    });

    println!("===== PIPELINE: SET TTL 10000 TIMES WITH SETNX AND LUA(ON EMPTY VALUES, ADDS TTL) \
              =====");
    measure(|| {
        let keys: Vec<_> = (0..10000)
            .map(|_| Uuid::new_v4().as_bytes().iter().cloned().collect::<Vec<_>>())
            .collect();
        let empty: Vec<u8> = vec![];

        let mut pipe = redis::pipe();

        for k in &keys {
            pipe.cmd("SETNX").arg(k.as_slice()).arg(empty.as_slice()).ignore();
            pipe.cmd("EVAL").arg(SET_TTL_LUA).arg(1).arg(k.as_slice()).arg(20).ignore();
        }

        pipe.execute(&conn);

        let mut pipe = redis::pipe();

        for k in &keys {
            pipe.get(k.as_slice());
            pipe.cmd("TTL").arg(k.as_slice());
        }

        let results: Vec<(Vec<u8>, i64)> = pipe.query(&conn).unwrap();
        for (vv, ttl) in results {
            assert_eq!(vv, empty.clone());
            assert!(ttl > 0);
        }
    });

    clear(&conn);

    println!("===== PIPELINE: SET TTL 10000 TIMES WITH SETNX AND LUA(ON NON EMPTY VALUES, DOES \
              NOT ADD TTL) =====");
    measure(|| {
        let keys: Vec<_> = (0..10000)
            .map(|_| Uuid::new_v4().as_bytes().iter().cloned().collect::<Vec<_>>())
            .collect();
        let v: Vec<u8> = vec![1];

        let mut pipe = redis::pipe();

        for k in &keys {
            pipe.cmd("SETNX").arg(k.as_slice()).arg(v.as_slice()).ignore();
            pipe.cmd("EVAL").arg(SET_TTL_LUA).arg(1).arg(k.as_slice()).arg(20).ignore();
        }

        pipe.execute(&conn);

        let mut pipe = redis::pipe();

        for k in &keys {
            pipe.get(k.as_slice());
            pipe.cmd("TTL").arg(k.as_slice());
        }

        let results: Vec<(Vec<u8>, i64)> = pipe.query(&conn).unwrap();
        for (vv, ttl) in results {
            assert_eq!(vv, v.clone());
            assert!(ttl == -1);
        }
    });

    clear(&conn);

}

fn measure<F>(f: F) -> ()
    where F: Fn() -> ()
{
    let start = time::Instant::now();
    f();
    let time = time::Instant::now() - start;
    println!("Took {} ms", duration_to_millis(&time));
}

fn clear(conn: &redis::Connection) {
    println!("===== Clear Redis =====");
    measure(|| {
        redis::cmd("FLUSHALL").execute(conn);
    });
}

fn hash<H: Hash>(t: &H) -> u64 {
    let mut hasher = DefaultHasher::new();
    t.hash(&mut hasher);
    hasher.finish()
}

fn duration_to_millis(d: &time::Duration) -> f64 {
    let secs = d.as_secs() as f64;
    let nanos = d.subsec_nanos() as f64;
    secs * 1000.0 + nanos / 1_000_000.0
}
