extern crate datalog;
use datalog::Iteration;

fn main() {

    let mut nodes = Vec::new();
    let mut edges = Vec::new();

    use std::io::{BufRead, BufReader};
    use std::fs::File;

    let filename = std::env::args().nth(1).unwrap();
    let file = BufReader::new(File::open(filename).unwrap());
    for readline in file.lines() {
        let line = readline.ok().expect("read error");
        if !line.starts_with('#') && line.len() > 0 {
            let mut elts = line[..].split_whitespace();
            let src: u32 = elts.next().unwrap().parse().ok().expect("malformed src");
            let dst: u32 = elts.next().unwrap().parse().ok().expect("malformed dst");
            let typ: &str = elts.next().unwrap();
            match typ {
                "n" => { nodes.push((dst, src)); },
                "e" => { edges.push((src, dst)); },
                unk => { panic!("unknown type: {}", unk)},
            }
        }
    }

    let mut iteration = Iteration::new();

    let variable1 = iteration.variable::<(u32,u32)>("nodes");
    let variable2 = iteration.variable::<(u32,u32)>("edges");

    variable1.insert(nodes.into());
    variable2.insert(edges.into());

    while iteration.changed() {

        // N(a,c) <-  N(a,b), E(b,c)
        variable1.from_join(&variable1, &variable2, |_b, &a, &c| (c,a));

    }

    let reachable = variable1.complete();

}