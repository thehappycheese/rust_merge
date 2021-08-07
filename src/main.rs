use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

use polars::prelude::*;

fn main() -> Result<()>{

    let segments_lazy_frame= 
        LazyCsvReader::new("K:\\2021 Working form home\\DataPreparation\\03 Segmentation\\03.01 - LCC_network_05082020.csv".into())
        .has_header(true)
        .finish()
        .select(&[
            col("RoadName").alias("road"),
            col("Cway").alias("cway"),
            (col("RoadName")+lit("_")+col("Cway")).alias("road_cway"),
            col("Name").alias("seg_name"),
            cast(col("From")*lit(1000), DataType::Int32).alias("slk_from"),
            cast(col("To")*lit(1000), DataType::Int32).alias("slk_to"),
        ]);
    // 0    Field { name: "road", data_type: Utf8 },
    // 1    Field{ name: "cway", data_type: Utf8 },
    // 2    Field{ name: "seg_name", data_type: Utf8 },
    // 3    Field{ name: "slk_length", data_type: Float64 },
    // 4    Field{ name: "slk_from", data_type: Int32 },
    // 5    Field{ name: "slk_to", data_type: Int32 }
    
    let data_lazy_frame= 
        LazyCsvReader::new("K:\\2021 Working form home\\DataPreparation\\07 TSD Data\\TSD Extract.All Roads.2020 Network Survey.2021-04-22.09-11.331903024.csv".into())
        .has_header(true)
        .finish()
        .select(&[
            //col("ROAD_NO").alias("road"),
            //col("CWAY").alias("cway"),
            col("DIRN").alias("dirn"),
            (col("ROAD_NO")+lit("_")+col("CWAY")).alias("road_cway"),
            (cast(col("START_SLK"),DataType::Int32)*lit(1000)).alias("slk_from"),
            (cast(col("END_SLK"), DataType::Int32)*lit(1000)).alias("slk_to"),
            
            col("D0").alias("deflection"),
            // col("D200"),
            (col("D0") - col("D200")).alias("curvature")
        ]);
    // 0    Field { name: "ROAD_NO", data_type: Utf8 },
    // 1    Field { name: "START_SLK", data_type: Float64 },
    // 2    Field { name: "END_SLK", data_type: Float64 }, 
    // 3    Field { name: "CWAY", data_type: Utf8 },
    // 4    Field{ name: "START_TRUE", data_type: Float64 },
    // 5    Field{ name: "END_TRUE", data_type: Float64 },
    // 6    Field{ name: "DIRN", data_type: Utf8 },
    // 7    Field{ name: "SURVEY_DATE", data_type: Utf8 },
    // 8    Field{ name: "SURF_TEMPERATURE", data_type: Float64 },
    // 9    Field{ name: "AIR_TEMPERATURE", data_type: Float64 },
    // 10   Field{ name: "SURVEY_SPEED", data_type: Int64 },
    // 11   Field{ name: "STRAIN_GAGUE_LEFT", data_type: Float64 },
    // 12   Field{ name: "STRAIN_GAGUE_RIGHT", data_type: Float64 },
    // 13   Field{ name: "SLP100", data_type: Utf8 },
    // 14   Field{ name: "SLP200", data_type: Utf8 },
    // 15   Field{ name: "SLP300", data_type: Utf8 },
    // 16   Field{ name: "SLP450", data_type: Utf8 },
    // 17   Field{ name: "SLP600", data_type: Utf8 },
    // 18   Field{ name: "SLP900", data_type: Utf8 },
    // 19   Field{ name: "STR_COND_IDX_200", data_type: Utf8 },
    // 20   Field{ name: "STR_COND_IDX_300", data_type: Utf8 },
    // 21   Field{ name: "STR_COND_IDX_SUBGRADE", data_type: Utf8 },
    // 22   Field{ name: "TD0", data_type: Float64 },
    // 23   Field{ name: "TD200", data_type: Float64 },
    // 24   Field{ name: "TD300", data_type: Utf8 },
    // 25   Field{ name: "TD450", data_type: Utf8 },
    // 26   Field{ name: "TD600", data_type: Utf8 },
    // 27   Field{ name: "TD900", data_type: Utf8 },
    // 28   Field{ name: "D0", data_type: Float64 },
    // 29   Field{ name: "D200", data_type: Float64 },
    // 30   Field{ name: "D300", data_type: Utf8 },
    // 31   Field{ name: "D450", data_type: Utf8 },
    // 32   Field{ name: "D600", data_type: Utf8 },
    // 33   Field{ name: "D750", data_type: Utf8 },
    // 34   Field{ name: "D900", data_type: Utf8 },
    // 35   Field{ name: "D1200", data_type: Utf8 },
    // 36   Field{ name: "D1500", data_type: Utf8 }
    
    let segments = segments_lazy_frame.collect()?;
    let data = data_lazy_frame.collect()?;

    let segments_groups     = segments.groupby("road_cway")?.groups()?;
    let data_groups         = data.groupby("road_cway")?.groups()?;

    
    let target_slk_from     = segments.column("slk_from")?;
    let target_slk_to       = segments.column("slk_to")?;

    let data_slk_from       = data.column("slk_from")?;
    let data_slk_to         = data.column("slk_to")?;
    
    let data_deflection     = data.column("deflection")?;
    let data_curvature      = data.column("curvature")?;

    

    let output_length = segments.height();

    let joined_groups       = segments_groups.inner_join(&data_groups, "road_cway", "road_cway")?;

    // let tab_road_cway = joined_groups.column("road_cway")?.utf8()?;
    // let tab_segments_rows = joined_groups.column("groups")?.list()?;
    // let tab_data_rows = joined_groups.column("groups_other")?.list()?;
    
    // izip!(&[tab_road_cway.into_iter(),tab_segments_rows.into_iter(),tab_data_rows.into_iter()]).map(|a,b,c|{
    //     a
    // });
        

    


    
   // let test_output:ChunkedBuilder<DataType::Float64> = ChunkedBuilder::new("test", output_length);
    


    Ok(())
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