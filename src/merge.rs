use polars::prelude::*;

pub enum Aggregation {
	KeepFirst,
    KeepLongest,
    LengthWeightedAverage,
    LengthWeightedPercentile(f32),
}

pub struct Column<'a> {
    name: &'a str,
    rename: &'a str,
    //output_type: DataType,
    aggregations: Aggregation,
	//string_bytes_size: usize
}

impl<'a> Column<'a> {
pub fn new(
        name: &'a str,
        rename: Option<&'a str>,
        //output_type: DataType,
        aggregation: Aggregation,
		//string_bytes_size: Option<usize>
    ) -> Column<'a> {
        Column {
            name: name,
            rename: match rename {
                None => name,
                Some(rename) => rename,
            },
            //output_type: output_type,
            aggregations: aggregation,
			// string_bytes_size: match string_bytes_size{
			// 	Some(x)=>x,
			// 	None=> 30
			// }
        }
    }
}

trait SeriesBuilderSupportedTypes{}

impl SeriesBuilderSupportedTypes for Float64Type{}


enum SeriesBuilder {
    F64(PrimitiveChunkedBuilder<Float64Type>),
	F32(PrimitiveChunkedBuilder<Float32Type>),
	
	I64(PrimitiveChunkedBuilder<Int64Type>),
    I32(PrimitiveChunkedBuilder<Int32Type>),
	I16(PrimitiveChunkedBuilder<Int16Type>),
	I8(PrimitiveChunkedBuilder<Int8Type>),

	U64(PrimitiveChunkedBuilder<UInt64Type>),
	U32(PrimitiveChunkedBuilder<UInt32Type>),
	U16(PrimitiveChunkedBuilder<UInt16Type>),
	U8(PrimitiveChunkedBuilder<UInt8Type>),

	UTF8(Utf8ChunkedBuilder),
}


pub fn keep_intervals(
    dataframe_target: DataFrame,
    dataframe_mergee: DataFrame,
    join_left: Vec<&str>,
    from_to: (&str, &str),
    columns: Vec<Column>,
) -> Result<DataFrame> {
    let (slk_from, slk_to) = from_to;

    let target_groups = dataframe_target.groupby(&join_left)?.groups()?;
    println!("did group by other");

    let mergee_groups = match dataframe_mergee.groupby(&join_left) {
        Ok(a) => match a.groups() {
            Ok(a) => a,
            Err(x) => {
                println!("whot");
                return Err(x);
            }
        },
        Err(x) => {
            println!("why");
            return Err(x);
        }
    };
    println!("target_groups");
    println!("{:?}", target_groups);
    println!("mergee_groups");
    println!("{:?}", mergee_groups);

    let output_length = dataframe_target.height();
    println!("output_length {:?}", output_length);

    let joined_groups =
        target_groups.join(&mergee_groups, &join_left, &join_left, JoinType::Inner)?;

    let mut joined_groups_target_rows = joined_groups
        .column("groups")
        .unwrap()
        .list()
        .unwrap()
        .into_iter();
    let mut joined_groups_mergee_rows = joined_groups
        .column("groups_right")
        .unwrap()
        .list()
        .unwrap()
        .into_iter();

    let segments_slk_from = dataframe_target.column(slk_from).unwrap().i32().unwrap();
    let segments_slk_to = dataframe_target.column(slk_to).unwrap().i32().unwrap();
    //let segments_slk_length = segments_slk_to - segments_slk_from;

    // let mut out_builder_index = Arc::new(PrimitiveChunkedBuilder::<UInt32Type>::new("segment_index", segments.height()));
    // let mut out_builder_curvature = Arc::new(PrimitiveChunkedBuilder::<Float64Type>::new("curvature", segments.height()));
    // let mut out_builder_deflection = Arc::new(PrimitiveChunkedBuilder::<Float64Type>::new("deflection", segments.height()));


    

    let mut output_columns: Vec<SeriesBuilder> = vec![];

    for column in columns {
        let mergee_column_index = match dataframe_mergee.find_idx_by_name(column.name){
            Some(index)=>index,
            None=>panic!("cannot find column name {} in mergee dataframe", column.name)
        };
        output_columns.push(match &dataframe_mergee.dtypes()[mergee_column_index] {
            DataType::Float64 => SeriesBuilder::F64(PrimitiveChunkedBuilder::<Float64Type>::new(column.name, dataframe_target.height())),
            DataType::Float32 => SeriesBuilder::F32(PrimitiveChunkedBuilder::<Float32Type>::new(column.name, dataframe_target.height())),
			
			DataType::Int64   => SeriesBuilder::I64(PrimitiveChunkedBuilder::<Int64Type>::new(column.name, dataframe_target.height())),
			DataType::Int32   => SeriesBuilder::I32(PrimitiveChunkedBuilder::<Int32Type>::new(column.name, dataframe_target.height())),
			DataType::Int16   => SeriesBuilder::I16(PrimitiveChunkedBuilder::<Int16Type>::new(column.name, dataframe_target.height())),
			DataType::Int8    => SeriesBuilder:: I8(PrimitiveChunkedBuilder::<Int8Type>::new (column.name, dataframe_target.height())),
			
			DataType::UInt64  => SeriesBuilder::U64(PrimitiveChunkedBuilder::<UInt64Type>::new(column.name, dataframe_target.height())),
			DataType::UInt32  => SeriesBuilder::U32(PrimitiveChunkedBuilder::<UInt32Type>::new(column.name, dataframe_target.height())),
			DataType::UInt16  => SeriesBuilder::U16(PrimitiveChunkedBuilder::<UInt16Type>::new(column.name, dataframe_target.height())),
			DataType::UInt8   => SeriesBuilder:: U8(PrimitiveChunkedBuilder::<UInt8Type>::new (column.name, dataframe_target.height())),
			
			DataType::Utf8    => SeriesBuilder::UTF8(Utf8ChunkedBuilder::new(
                column.name,
                dataframe_target.height(),
                dataframe_mergee.get_columns()[mergee_column_index].utf8().unwrap().array_data().iter().map(|array_data|array_data.get_buffer_memory_size()).sum())
            ),
			
			some_other_type=>{
                // TODO... are there any types which could actually crop up here?
				panic!("Unsupported type {:?}", some_other_type);
			}
        });
    }

    //let a = output_columns[0];

    // Loop over joined_groups
    let k: Vec<_> = (0..10)
        .map(|index| {
            println!("{:?}", index);
            let segments_rows = joined_groups_target_rows.next().unwrap().unwrap();
            let data_rows = joined_groups_mergee_rows.next().unwrap().unwrap();

            // Get data to be joined against this group of segments.
            let data_matching_target_group = dataframe_mergee.take(data_rows.u32().unwrap()); // We don't need to do this: we can just take the values needed from the slk_from and slk_to columns...
            let data_matching_target_group_slk_from = data_matching_target_group.column(slk_from).unwrap();
            let data_matching_target_group_slk_to = data_matching_target_group.column(slk_to).unwrap();

            let data_matching_target_group_deflection = data_matching_target_group.column("deflection").unwrap();
            let data_matching_target_group_slk_to = data_matching_target_group.column("curvature").unwrap();

            // Loop over segments
            let poop: () = segments_rows
                .u32()
                .unwrap()
                .map(|segment_index| {
                    println!("segment index{:?}", segment_index);
                    let segment_slk_from = segments_slk_from.get(segment_index as usize).unwrap();
                    let segment_slk_to = segments_slk_to.get(segment_index as usize).unwrap();
                    // Get data to be joined to THIS segment;

                    // then down here we just take the indicies from data_rows... and finally apply another take to the desired columns in the final df.
                    let dat = data_matching_target_group
                        .filter(
                            &(data_matching_target_group_slk_from.lt(segment_slk_to)
                                & data_matching_target_group_slk_to.gt(segment_slk_from)),
                        )
                        .unwrap();
                    if dat.width() * dat.height() > 0 {
                        println!("=================");
                        println!(
                            "For target index {:?} from slk {:?} to slk {:?}",
                            index, segment_slk_from, segment_slk_to
                        );
                        println!("so, we get this dataframe:");
                        println!("{:?}", dat);

                        // TODO: compute overlap
                        // out_builder_index.append_value(segment_index);
                        // out_builder_deflection.append_value(dat.column("deflection").unwrap().mean().unwrap());
                        // out_builder_curvature.append_value(dat.column("curvature").unwrap().mean().unwrap())
                    }
                    ()
                })
                .unwrap()
                .collect();

            // let out_index = out_builder_index.finish();
            // let out_deflection = out_builder_deflection.finish();
            // let out_curvature = out_builder_curvature.finish();

            let segments_to_match = dataframe_target.take(segments_rows.slice(0, 3).u32().unwrap());

            0
        })
        .collect();

    Ok(dataframe_target)
}
