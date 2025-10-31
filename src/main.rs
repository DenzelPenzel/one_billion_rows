use anyhow::{Result, bail};
use clap::{Parser, arg};

#[derive(Parser, Debug)]
struct Args {
    #[arg(long, default_value = "measurements.txt")]
    name: String,

    #[arg(long, default_value = "")]
    cpuprofile: String,

    #[arg(long, default_value = "")]
    mem_profile: String,

    #[arg(long, default_value = "")]
    exec_profile: String,
}

fn main() -> Result<()> {
    let args = Args::parse();

    if args.name.is_empty() {
        bail!("Filename param is missing");
    }

    let input_path = format!("./data/{}", args.name);
    sol1::solve(input_path).map_err(|e| anyhow::anyhow!("{}", e))?;

    Ok(())
}
