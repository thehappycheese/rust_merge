

use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;


use polars::prelude::*;

//use polars::frame::DataFrame;

fn main() -> Result<()>{
    println!("Hello, world!");
    let mut segments=  get_csv("")?;
    segments.set_column_names(&["road","cway","seg_name","slk_from","slk_to","slk_length"])?;
    
    let slk_from = (segments.drop_in_place("slk_from")?.f64()?*1000.0).into_series().cast_with_dtype(&DataType::Int32)?;
    let slk_to = (segments.drop_in_place("slk_to")?.f64()?*1000.0).into_series().cast_with_dtype(&DataType::Int32)?;
    segments = segments.hstack(&[slk_from, slk_to])?;

    println!("{:?}", segments);
    println!("{:?}", segments.fields());
    

    let data=  get_csv("")?;
    println!("{:?}", data);
    println!("{:?}", data.fields());


//     let data= LazyCsvReader::new("".into())
//     .has_header(true)
//     .with_delimiter(b',')
//     .finish();
// let result = data.sort("START_SLK", true).fetch(100)?;
// print!("{:?}",result);

    
    //let _df = merge(segments, data);
    Ok(())
}


fn get_csv<PathType>(path:PathType) -> Result<DataFrame> 
    where PathType:Into<std::path::PathBuf>{
    CsvReader::from_path(path)?.infer_schema(None).has_header(true).finish()
}

fn merge(target:DataFrame, data:DataFrame) -> Result<DataFrame>{
    let data_group_indecies = data.groupby(&["road_no","cway"]).unwrap().groups().unwrap();
    
    //let target_group_by = target.groupby(&["road_no","cway"]).unwrap();


    
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