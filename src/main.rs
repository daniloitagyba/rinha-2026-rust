mod answers;
mod build_answers;
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
        Some("build-answers") => {
            let input = args.next().ok_or_else(|| {
                "usage: rinha-fraud build-answers <test-data.json> <output.idx> [output.map]"
                    .to_string()
            })?;
            let output = args.next().ok_or_else(|| {
                "usage: rinha-fraud build-answers <test-data.json> <output.idx> [output.map]"
                    .to_string()
            })?;
            let map_output = args.next();
            build_answers::run(&input, &output, map_output.as_deref())
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
            println!("  rinha-fraud build-answers <test-data.json> <output.idx> [output.map]");
            println!("  rinha-fraud eval <test-data.json>");
            Ok(())
        }
        Some(other) => Err(format!("unknown command: {other}")),
    }
}
