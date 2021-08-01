

use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;


use polars::prelude::{self as pl, ChunkTake, SerReader};
use polars::frame::DataFrame;

fn main() -> Result<(), Box<dyn std::error::Error>>{
    println!("Hello, world!");
    let segments=  get_csv("./test_data/segments.csv")?;
    println!("{:?}", segments);

    let data=  get_csv("./test_data/data.csv")?;
    println!("{:?}", data);
    merge(segments, data);
    Ok(())
}


fn get_csv<PathType>(path:PathType) -> Result<DataFrame, Box<dyn std::error::Error>> 
    where PathType:Into<std::path::PathBuf>{
    match pl::CsvReader::from_path(path)?.infer_schema(None).has_header(true).finish(){
        Ok(df)=>Ok(df),
        Err(e)=>{
            println!("failed to load {}",e);
            return Err(Box::new(e));
        }
    }
}

fn merge(target:DataFrame, data:DataFrame) -> Result<DataFrame, Box<dyn std::error::Error>>{
    let data_group_indecies = data.groupby(&["road_no","cway"]).unwrap().groups().unwrap();
    
    let target_group_by = target.groupby(&["road_no","cway"]).unwrap();

    target_group_by.
    
    data_group_indecies.column("groups").unwrap()
        .list().unwrap()
        .into_iter().filter_map(|item| item)
        .for_each(|item|{
        println!("{:?}",item);
        let ss = data.column("category").unwrap().take(item.u32().unwrap());
        println!("{:?}",ss)
    });

    Ok(target)

}