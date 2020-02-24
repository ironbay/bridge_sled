use rustler::{Encoder, Env, Error, ResourceArc, Term};
use sled::Batch;
use sled::Db;
use std::string::String;
use std::sync::Mutex;

mod atoms {
    rustler::rustler_atoms! {
        atom ok;
        atom done;
        //atom error;
        //atom __true__ = "true";
        //atom __false__ = "false";
    }
}

rustler::rustler_export_nifs! {
    "Elixir.Bridge.Sled",
    [
        ("db_open", 1, db_open),
        ("db_insert", 3, db_insert),
        ("db_get", 2, db_get),
        ("db_apply_batch", 2, db_apply_batch),
        ("db_range", 4, db_range),
        ("batch_default", 0, batch_default),
        ("batch_insert", 3, batch_insert),
        ("batch_remove", 2, batch_remove),
    ],
    Some(on_init)
}

pub struct Wrapped<T> {
    pub value: T,
}

impl<T> Wrapped<T> {
    pub fn new(value: T) -> Wrapped<T> {
        Wrapped { value: value }
    }
}

fn on_init<'a>(env: Env<'a>, _load_info: Term<'a>) -> bool {
    rustler::resource_struct_init!(Wrapped<Db>, env);
    rustler::resource_struct_init!(Wrapped<Mutex<Batch>>, env);
    true
}

fn db_open<'a>(env: Env<'a>, args: &[Term<'a>]) -> Result<Term<'a>, Error> {
    let path: &str = args[0].decode()?;
    let db = sled::Config::default()
        .path(path)
        .cache_capacity(10_000_000_000)
        .flush_every_ms(Some(1000))
        .snapshot_after_ops(100_000)
        // .use_compression(true)
        .open()
        .unwrap();
    Ok((atoms::ok(), ResourceArc::new(Wrapped::new(db))).encode(env))
}

fn db_insert<'a>(env: Env<'a>, args: &[Term<'a>]) -> Result<Term<'a>, Error> {
    let wrapped: ResourceArc<Wrapped<Db>> = args[0].decode()?;
    let db = &wrapped.value;
    let key: &str = args[1].decode()?;
    let value: &str = args[2].decode()?;
    db.insert(key, value).unwrap();
    Ok(atoms::ok().encode(env))
}

fn db_get<'a>(env: Env<'a>, args: &[Term<'a>]) -> Result<Term<'a>, Error> {
    let wrapped: ResourceArc<Wrapped<Db>> = args[0].decode()?;
    let db: &Db = &wrapped.value;
    let key: &str = args[1].decode()?;
    match db.get(key).unwrap() {
        Some(result) => Ok((
            atoms::ok().encode(env),
            String::from_utf8(result.to_vec()).unwrap(),
        )
            .encode(env)),
        _ => Ok((atoms::ok(), "").encode(env)),
    }
}

fn db_range<'a>(env: Env<'a>, args: &[Term<'a>]) -> Result<Term<'a>, Error> {
    let wrapped: ResourceArc<Wrapped<Db>> = args[0].decode()?;
    let db: &Db = &wrapped.value;
    let min: &str = args[1].decode()?;
    let max: &str = args[2].decode()?;
    let take: usize = args[3].decode()?;
    let results: std::vec::Vec<(String, String)> = db
        .range::<&str, std::ops::Range<&str>>(min..max)
        .take(take)
        .map(|result| {
            let (key, value) = result.unwrap();
            (
                String::from_utf8(key.to_vec()).unwrap(),
                String::from_utf8(value.to_vec()).unwrap(),
            )
        })
        .collect();
    let mut items = Vec::<(&str, &str)>::new();
    for item in &results {
        items.push((&item.0, &item.1));
    }
    match results.len() {
        0 => Ok(atoms::done().encode(env)),
        _ => Ok((atoms::ok(), (&results.last().unwrap().0, items)).encode(env)),
    }
}

fn db_apply_batch<'a>(env: Env<'a>, args: &[Term<'a>]) -> Result<Term<'a>, Error> {
    let wrapped_db: ResourceArc<Wrapped<Db>> = args[0].decode()?;
    let db: &Db = &wrapped_db.value;
    let wrapped_batch: ResourceArc<Wrapped<Mutex<Batch>>> = args[1].decode()?;
    let batch: std::sync::MutexGuard<Batch> = wrapped_batch.value.lock().unwrap();
    db.apply_batch(batch.clone()).unwrap();
    Ok(atoms::ok().encode(env))
}

fn batch_default<'a>(env: Env<'a>, _args: &[Term<'a>]) -> Result<Term<'a>, Error> {
    let batch = Batch::default();
    Ok((
        atoms::ok(),
        ResourceArc::new(Wrapped::new(Mutex::new(batch))),
    )
        .encode(env))
}

fn batch_insert<'a>(env: Env<'a>, args: &[Term<'a>]) -> Result<Term<'a>, Error> {
    let wrapped: ResourceArc<Wrapped<Mutex<Batch>>> = args[0].decode()?;
    let mut batch = wrapped.value.lock().unwrap();
    let key: &str = args[1].decode()?;
    let value: &str = args[2].decode()?;
    batch.insert(key, value);
    Ok(atoms::ok().encode(env))
}

fn batch_remove<'a>(env: Env<'a>, args: &[Term<'a>]) -> Result<Term<'a>, Error> {
    let wrapped: ResourceArc<Wrapped<Mutex<Batch>>> = args[0].decode()?;
    let mut batch = wrapped.value.lock().unwrap();
    let key: &str = args[1].decode()?;
    batch.remove(key);
    Ok(atoms::ok().encode(env))
}
