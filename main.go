package main

import (
	"fmt"
	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"
	"os"
	"time"
)

var (
	EndPoint     = "https://dianmo-data.cn-hangzhou.tablestore.aliyuncs.com"
	InstanceName = "dianmo-data"
)

func main() {
	timeseriesClient := tablestore.NewTimeseriesClient(EndPoint, InstanceName, os.Getenv("AK"), os.Getenv("SK"))
	timeseriesTableName := "dianmo"
	DescribeTimeseriesTableSample(timeseriesClient, timeseriesTableName)
	PutTimeseriesDataSample(timeseriesClient, timeseriesTableName, "123")
}

// DescribeTimeseriesTableSample 查询详情
func DescribeTimeseriesTableSample(timeseriesClient *tablestore.TimeseriesClient, timeseriesTableName string) {
	fmt.Println("[Info]: Begin to require timeseries table description!")
	describeTimeseriesTableRequest := tablestore.NewDescribeTimeseriesTableRequset(timeseriesTableName) // 构造请求，并设置时序表名。

	describeTimeseriesTableResponse, err := timeseriesClient.DescribeTimeseriesTable(describeTimeseriesTableRequest)
	if err != nil {
		fmt.Println("[Error]: Failed to require timeseries table description!")
		CreateTimeseriesTableSample(timeseriesClient, timeseriesTableName, 864000)
		return
	}
	fmt.Println("[Info]: DescribeTimeseriesTableSample finished. Timeseries table meta: ")
	fmt.Println("[Info]: TimeseriesTableName: ", describeTimeseriesTableResponse.GetTimeseriesTableMeta().GetTimeseriesTableName())
	fmt.Println("[Info]: TimeseriesTable TTL: ", describeTimeseriesTableResponse.GetTimeseriesTableMeta().GetTimeseriesTableOPtions().GetTimeToLive())
}

// CreateTimeseriesTableSample 创建时序表
func CreateTimeseriesTableSample(client *tablestore.TimeseriesClient, timeseriesTableName string, timeToLive int64) {
	fmt.Println("[Info]: Begin to create timeseries table: ", timeseriesTableName)

	timeseriesTableOptions := tablestore.NewTimeseriesTableOptions(timeToLive) // 构造时序表配置信息。

	// 构造表元数据信息
	timeseriesTableMeta := tablestore.NewTimeseriesTableMeta(timeseriesTableName) // 设置时序表名。
	timeseriesTableMeta.SetTimeseriesTableOptions(timeseriesTableOptions)         // 设置时序表配置信息

	createTimeseriesTableRequest := tablestore.NewCreateTimeseriesTableRequest() // 构造创建时序表请求。
	createTimeseriesTableRequest.SetTimeseriesTableMeta(timeseriesTableMeta)

	createTimeseriesTableResponse, err := client.CreateTimeseriesTable(createTimeseriesTableRequest) // 调用client创建时序表。
	if err != nil {
		fmt.Println("[Error]: Failed to create timeseries table with error: ", err)
		return
	}
	fmt.Println("[Info]: CreateTimeseriesTable finished! RequestId: ", createTimeseriesTableResponse.RequestId)
}

// PutTimeseriesDataSample 上传数据
func PutTimeseriesDataSample(client *tablestore.TimeseriesClient, timeseriesTableName string, tripId string) {
	fmt.Println("[Info]: Begin to PutTimeseriesDataSample !")

	// 构造时序数据行timeseriesRow。
	timeseriesKey := tablestore.NewTimeseriesKey()
	// 记录类型: 电动摩托车
	timeseriesKey.SetMeasurementName("dianmo")
	// 车辆ID
	timeseriesKey.SetDataSource("61007953")
	// 行程ID
	timeseriesKey.AddTag("tripId", tripId)
	// 颜色
	timeseriesKey.AddTag("color", "深灰")
	// 车牌号
	timeseriesKey.AddTag("license", "豫A123456")

	timeseriesRow := tablestore.NewTimeseriesRow(timeseriesKey)
	timeseriesRow.SetTimeInus(time.Now().UnixNano() / 1000)
	// 经纬度
	timeseriesRow.AddField("location", tablestore.NewColumnValue(tablestore.ColumnType_STRING, "200,100"))
	// 总里程
	timeseriesRow.AddField("miles", tablestore.NewColumnValue(tablestore.ColumnType_DOUBLE, 5.0))
	// 速度
	timeseriesRow.AddField("speed", tablestore.NewColumnValue(tablestore.ColumnType_DOUBLE, 55.0))
	// 电量
	timeseriesRow.AddField("dianliang", tablestore.NewColumnValue(tablestore.ColumnType_DOUBLE, 72.0))

	// 构造写入时序数据的请求。
	putTimeseriesDataRequest := tablestore.NewPutTimeseriesDataRequest(timeseriesTableName)
	putTimeseriesDataRequest.AddTimeseriesRows(timeseriesRow)

	// 调用时序客户端写入时序数据。
	putTimeseriesDataResponse, err := client.PutTimeseriesData(putTimeseriesDataRequest)
	if err != nil {
		fmt.Println("[Error]: Put timeseries data Failed with error: ", err)
		return
	}
	if len(putTimeseriesDataResponse.GetFailedRowResults()) > 0 {
		fmt.Println("[Warning]: Put timeseries data finished ! Some of timeseries row put Failed: ")
		for i := 0; i < len(putTimeseriesDataResponse.GetFailedRowResults()); i++ {
			FailedRow := putTimeseriesDataResponse.GetFailedRowResults()[i]
			fmt.Println("[Warning]: Failed Row: Index: ", FailedRow.Index, " Error: ", FailedRow.Error)
		}
	} else {
		fmt.Println("[Info]: PutTimeseriesDataSample finished! RequestId: ", putTimeseriesDataResponse.RequestId)
	}
}
