mod build_index;
mod eval;
#[cfg(unix)]
mod fdpass;
mod http;
mod index;
mod known_ids;
mod parser;
#[cfg(unix)]
mod raw_server;
mod reference_tools;
#[cfg(unix)]
mod rpc_server;
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
        Some("eval-references") => {
            let input = args.next().ok_or_else(|| {
                "usage: rinha-fraud eval-references <references.json>".to_string()
            })?;
            reference_tools::eval_references(&input)
        }
        Some("split-references") => {
            let train = args.next().ok_or_else(|| {
                "usage: rinha-fraud split-references <train.json> <holdout.json> [modulus] [offset]"
                    .to_string()
            })?;
            let holdout = args.next().ok_or_else(|| {
                "usage: rinha-fraud split-references <train.json> <holdout.json> [modulus] [offset]"
                    .to_string()
            })?;
            let modulus = args
                .next()
                .as_deref()
                .unwrap_or("100")
                .parse::<usize>()
                .map_err(|_| "modulus must be a positive integer".to_string())?;
            let offset = args
                .next()
                .as_deref()
                .unwrap_or("0")
                .parse::<usize>()
                .map_err(|_| "offset must be a non-negative integer".to_string())?;
            reference_tools::split_references(&train, &holdout, modulus, offset)
        }
        Some("serve") | None => http::serve(),
        Some("--help") | Some("-h") => {
            println!("usage:");
            println!("  rinha-fraud serve");
            println!("  rinha-fraud build-index <output.idx> < references.json");
            println!("  rinha-fraud eval <test-data.json>");
            println!("  rinha-fraud eval-references <references.json>");
            println!(
                "  rinha-fraud split-references <train.json> <holdout.json> [modulus] [offset]"
            );
            Ok(())
        }
        Some(other) => Err(format!("unknown command: {other}")),
    }
}
