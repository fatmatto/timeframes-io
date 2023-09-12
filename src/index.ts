import { TimeFrame } from "@apio/timeframes"

import { Float64, Table, TypeMap, Vector, tableFromIPC, tableToIPC,  Schema,
  vectorFromArray,
  RecordBatch,
  Field
 } from 'apache-arrow'
import {
  readParquet,
  writeParquet,
  Compression,
  WriterPropertiesBuilder
} from 'parquet-wasm/node/arrow1'

type VectorsMap<T extends TypeMap> = { [P in keyof T]: Vector<T[P]> };
type Columns = Record<string, any>

/**
 * Returns a new TimeFrame from an arrow dataset
 * @param data Data in Arrow table format
 */
export function fromArrow(data: Table): TimeFrame {
  return new TimeFrame({ data: data.toArray() })
}


/**
 * Converts a TimeFrame to Arrow format
 * @param tf The timeframe to convert to Arrow
 * @returns Arrow Table
 */
export function toArrow(tf: TimeFrame): Table {
  const fields = []
  const vectors = {} as VectorsMap<any>;
  const columns: Columns = { time: null }
  const rows = tf.rows()

  rows.forEach(row => {
    for (const colName in row) {
      columns[colName] = columns[colName] || []
      columns[colName].push(row[colName])
    }
  })

  for (const colName in columns) {
    if (colName === 'time') {
      fields.push(Field.new({ name: 'time', type: new Float64(), nullable: true }))
      vectors[colName] = vectorFromArray(columns[colName].map(t => new Date(t).getTime()))
    } else {
      fields.push(Field.new({ name: colName, type: new Float64(), nullable: true }))
      vectors[colName] = vectorFromArray(columns[colName])

    }
  } 

  const schema = new Schema(fields)
  const batchDefinition = {}
  for (const colName in vectors) {
    batchDefinition[colName] = vectors[colName].data[0]
  }
  const batches = [
    new RecordBatch(batchDefinition),
  ]
  
  return new Table<any>(schema, batches)
}

/**
 * Returns a new TimeFrame from a Parquet dataset
 * @param buffer Parquet buffer
 */
export function fromParquet(buffer: Buffer): TimeFrame {
  return new TimeFrame({ data: tableFromIPC(readParquet(buffer)).toArray() })
}


/**
 * Converts a TimeFrame to Parquet data format
 * @param tf 
 */
export function toParquet(tf: TimeFrame): Buffer {
  const writerProperties = new WriterPropertiesBuilder()
    .setCompression(Compression.ZSTD)
    .build()
  return Buffer.from(writeParquet(
    tableToIPC(toArrow(tf), 'stream'),
    writerProperties
  ))
}