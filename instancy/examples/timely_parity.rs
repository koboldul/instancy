//! Run the finite native scenarios with `-- all` or `-- <scenario>`.
mod timely;

fn main() -> instancy::Result<()> {
    let mut args = std::env::args().skip(1);
    let scenario = args.next().unwrap_or_else(|| "all".to_owned());
    if args.next().is_some() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "expected one scenario name or all",
        )
        .into());
    }
    timely::run_named(&scenario)
}
