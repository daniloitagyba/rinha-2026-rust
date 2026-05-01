mod build_index;
mod eval;
mod http;
mod index;
mod parser;
mod vector;

use std::env;
use std::process;

fn main() {
    if let Err(err) = run() {
        eprintln!("{err}");
        process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let mut args = env::args();
    let _bin = args.next();

    match args.next().as_deref() {
        Some("build-index") => {
            let output = args
                .next()
                .ok_or_else(|| "usage: rinha-fraud build-index <output.idx>".to_string())?;
            build_index::run(&output)
        }
        Some("eval") => {
            let input = args
                .next()
                .ok_or_else(|| "usage: rinha-fraud eval <test-data.json>".to_string())?;
            eval::run(&input)
        }
        Some("serve") | None => http::serve(),
        Some("--help") | Some("-h") => {
            println!("usage:");
            println!("  rinha-fraud serve");
            println!("  rinha-fraud build-index <output.idx> < references.json");
            println!("  rinha-fraud eval <test-data.json>");
            Ok(())
        }
        Some(other) => Err(format!("unknown command: {other}")),
    }
}
