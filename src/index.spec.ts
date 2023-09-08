import test from "ava";
import { fromArrow, fromParquet, toArrow, toParquet } from ".";

import { TimeFrame } from "@apio/timeframes";

test('fromParquet() and toParquet() should correctly build a timeframe', async t => {
  const data = [
    { time: "2021-01-01", energy: 1, power: 5 },
    { time: "2021-01-02", energy: 2, power: 8 }
  ];
  const tf = new TimeFrame({ data });


  const parquetBuffer = toParquet(tf)

  const tf2 = await fromParquet(parquetBuffer)
  const row = tf2.atTime('2021-01-01')
  t.is(row !== null,true)

  if (row !== null) {
    t.is(row.energy, 1)
    t.is(row.power, 5)
  }

  t.is(tf2.rows().length, 2)
})

// test('fromParquet() Should correctly filter columns', async t => {
//   const data = [
//     { time: "2021-01-01", energy: 1, power: 5 },
//     { time: "2021-01-02", energy: 2, power: 8 }
//   ];
//   const tf = new TimeFrame({ data });


//   const parquetBuffer = toParquet(tf)

//   const tf2 = await fromParquet(parquetBuffer,['energy','time'])
//   console.log("BRILLOCCO",tf2.columnNames)
//   t.is(tf2.columnNames.length,1)
// })

test('fromArrow() and toArrow() should correctly build a timeframe', async t => {
  const data = [
    { time: "2021-01-01", energy: 1, power: 5 },
    { time: "2021-01-02", energy: 2, power: 8 },
  ];
  const tf = new TimeFrame({ data });


  const arrowBuffer = toArrow(tf)

  const tf2 = fromArrow(arrowBuffer)

  t.is(tf2.rows().length, 2)
  const row = tf2.atTime('2021-01-01')
  t.is(row !== null, true)
  if (row !== null) {
    t.is(row.energy, 1)
    t.is(row.power, 5)

  }
})