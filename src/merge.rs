
use polars::prelude::*;

pub enum Aggregation{
    KeepLongest,
    LengthWeightedAverage,
    LengthWeightedPercentile(f32),
}

pub struct Column<'a>{
    name:&'a str,
    rename:&'a str,
    output_type:DataType,
    aggregations:Aggregation
}

impl<'a> Column<'a> {
    pub fn new(name:&'a str, rename:Option<&'a str>, output_type:DataType, aggregation:Aggregation) -> Column<'a>{
        Column{
            name:name,
            rename:match rename {
                None=>name,
                Some(rename)=>rename
            },
            output_type:output_type,
            aggregations:aggregation
        }
    }
    
}



pub fn keep_intervals(
    segments:DataFrame,
    data:DataFrame,
    join_left:Vec<&str>,
    from_to:(&str, &str),
    columns:Vec<Column>
) -> Result<DataFrame>{
	
    let segments_groups     = segments.groupby(&join_left)?.groups()?;
	let data_groups         = data.groupby(&join_left)?.groups()?;

	
	let output_length = segments.height();
	
    // TODO: start from here: we tried to inner_join by join_left but inner_join will not accept multiple keys.

	let joined_groups       = segments_groups.inner_join(&data_groups, &join_left, &join_left)?;
	let mut joined_groups_road_cway = joined_groups.column("road_cway").unwrap().utf8().unwrap().into_iter();
	let mut joined_groups_segments_rows = joined_groups.column("groups").unwrap().list().unwrap().into_iter();
	let mut joined_groups_data_rows = joined_groups.column("groups_right").unwrap().list().unwrap().into_iter();


	
	let segments_slk_from = segments.column("slk_from").unwrap().i32().unwrap();
	let segments_slk_to = segments.column("slk_to").unwrap().i32().unwrap();
	
	let mut out_builder_index = Arc::new(PrimitiveChunkedBuilder::<UInt32Type>::new("segment_index", segments.height()));
	let mut out_builder_curvature = Arc::new(PrimitiveChunkedBuilder::<Float64Type>::new("curvature", segments.height()));
	let mut out_builder_deflection = Arc::new(PrimitiveChunkedBuilder::<Float64Type>::new("deflection", segments.height()));
	
	// Loop over joined_groups
	let k:Vec<_> = (0..10).map(|index|{
		let road_cway = joined_groups_road_cway.next().unwrap().unwrap();
		let segments_rows = joined_groups_segments_rows.next().unwrap().unwrap();
		let data_rows = joined_groups_data_rows.next().unwrap().unwrap();
		
		// Get data to be joined against this group of segments.
		let data_matching_target_group = data.take(data_rows.u32().unwrap()); // We dont need to do this: we can just take the values needed from the slk_from and slk_to columns...
		let data_matching_target_group_slk_from = data_matching_target_group.column("slk_from").unwrap();
		let data_matching_target_group_slk_to = data_matching_target_group.column("slk_to").unwrap();
		
		let data_matching_target_group_deflection = data_matching_target_group.column("deflection").unwrap();
		let data_matching_target_group_slk_to = data_matching_target_group.column("curvature").unwrap();

		// Loop over segments
		let poop:() = segments_rows.u32().unwrap().map(|segment_index|{
			
			let segment_slk_from = segments_slk_from.get(segment_index as usize).unwrap();
			let segment_slk_to = segments_slk_to.get(segment_index as usize).unwrap();
			// Get data to be joined to THIS segment;
			
			// then down here we just take the indicies from data_rows... and finally apply another take to the desired columns in the final df.
			let dat = data_matching_target_group.filter(
				&(data_matching_target_group_slk_from.lt(segment_slk_to) &
				data_matching_target_group_slk_to.gt(segment_slk_from))
			).unwrap();
			if !dat.is_empty(){
				println!("=================");
				println!("For target index {:?} ({:?}) from slk {:?} to slk {:?}",index, road_cway, segment_slk_from, segment_slk_to);
				println!("so, we get this dataframe:");
				println!("{:?}", dat);



				// TODO: compute overlap
				out_builder_index.append_value(segment_index);
				out_builder_deflection.append_value(dat.column("deflection").unwrap().mean().unwrap());
				out_builder_curvature.append_value(dat.column("curvature").unwrap().mean().unwrap())
			}
			()

		}).unwrap().collect();

		let out_index = out_builder_index.finish();
		let out_deflection = out_builder_deflection.finish();
		let out_curvature = out_builder_curvature.finish();
		

		let segments_to_match = segments.take(segments_rows.slice(0,3).u32().unwrap());

		println!("{:?}", road_cway);
		
		0
	}).collect();

	Ok(segments)

}