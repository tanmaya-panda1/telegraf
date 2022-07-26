package azure_data_explorer

import (
	"context"
	"io"
	"io/ioutil"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-kusto-go/kusto"
	"github.com/Azure/azure-kusto-go/kusto/ingest"
	"github.com/influxdata/telegraf"
	telegrafJson "github.com/influxdata/telegraf/plugins/serializers/json"
	"github.com/influxdata/telegraf/testutil"
	"github.com/stretchr/testify/require"
)

func TestWrite(t *testing.T) {
	t.Parallel()
	metricName := "test1"
	fakeClient := kusto.NewMockClient()
	expectedResultMap := map[string]string{metricName: `{"fields":{"value":1},"name":"test1","tags":{"tag1":"value1"},"timestamp":1257894000}`}
	mockMetrics := testutil.MockMetrics()
	// Multi tables
	mockMetrics2 := testutil.TestMetric(1.0, "test2")
	mockMetrics3 := testutil.TestMetric(2.0, "test3")
	mockMetricsMulti := make([]telegraf.Metric, 2)
	mockMetricsMulti[0] = mockMetrics2
	mockMetricsMulti[1] = mockMetrics3
	expectedResultMap2 := map[string]string{"test2": `{"fields":{"value":1.0},"name":"test2","tags":{"tag1":"value1"},"timestamp":1257894000}`, "test3": `{"fields":{"value":2.0},"name":"test3","tags":{"tag1":"value1"},"timestamp":1257894000}`}
	// List of tests
	testCases := []struct {
		name                      string
		inputMetric               []telegraf.Metric
		client                    *kusto.Client
		metricsGrouping           string
		tableNameToExpectedResult map[string]string
		expectedWriteError        string
		createTables              bool
		ingestionType             string
	}{
		{
			name:                      "Valid metric",
			inputMetric:               mockMetrics,
			createTables:              true,
			client:                    fakeClient,
			metricsGrouping:           tablePerMetric,
			tableNameToExpectedResult: expectedResultMap,
		},
		{
			name:                      "Don't create tables'",
			inputMetric:               mockMetrics,
			createTables:              false,
			client:                    fakeClient,
			metricsGrouping:           tablePerMetric,
			tableNameToExpectedResult: expectedResultMap,
		},
		{
			name:                      "SingleTable metric grouping type",
			inputMetric:               mockMetrics,
			createTables:              true,
			client:                    fakeClient,
			metricsGrouping:           singleTable,
			tableNameToExpectedResult: expectedResultMap,
		},
		{
			name:                      "Valid metric managed ingestion",
			inputMetric:               mockMetrics,
			createTables:              true,
			client:                    fakeClient,
			metricsGrouping:           tablePerMetric,
			tableNameToExpectedResult: expectedResultMap,
			ingestionType:             managedIngestion,
		},
		{
			name:                      "Table per metric type",
			inputMetric:               mockMetricsMulti,
			createTables:              true,
			client:                    fakeClient,
			metricsGrouping:           tablePerMetric,
			tableNameToExpectedResult: expectedResultMap2,
		},
	}

	for _, tC := range testCases {
		t.Run(tC.name, func(t *testing.T) {
			serializer, err := telegrafJson.NewSerializer(time.Second, "", "")
			require.NoError(t, err)
			for tN, jV := range tC.tableNameToExpectedResult {
				ingestionType := "queued"
				if tC.ingestionType != "" {
					ingestionType = tC.ingestionType
				}
				mockIngestor := &mockIngestor{}
				plugin := AzureDataExplorer{
					Endpoint:        "someendpoint",
					Database:        "databasename",
					Log:             testutil.Logger{},
					IngestionType:   ingestionType,
					MetricsGrouping: tC.metricsGrouping,
					TableName:       tN,
					CreateTables:    tC.createTables,
					client:          tC.client,
					ingesters: map[string]ingest.Ingestor{
						tN: mockIngestor,
					},
					serializer: serializer,
				}
				errorInWrite := plugin.Write(tC.inputMetric)
				if tC.expectedWriteError != "" {
					require.EqualError(t, errorInWrite, tC.expectedWriteError)
				} else {
					require.NoError(t, errorInWrite)
					createdIngestor := plugin.ingesters[tN]
					if tC.metricsGrouping == singleTable {
						createdIngestor = plugin.ingesters[tN]
					}
					records := mockIngestor.records[0] // the first element
					require.NotNil(t, createdIngestor)
					require.JSONEq(t, jV, records)
				}
			}

		})
	}
}

func TestSampleConfig(t *testing.T) {
	fakeClient := kusto.NewMockClient()
	plugin := AzureDataExplorer{
		Log:       testutil.Logger{},
		Endpoint:  "someendpoint",
		Database:  "databasename",
		client:    fakeClient,
		ingesters: make(map[string]ingest.Ingestor),
	}
	sampleConfig := plugin.SampleConfig()
	require.NotNil(t, sampleConfig)
	b, err := ioutil.ReadFile("sample.conf") // just pass the file name
	require.Nil(t, err)                      // read should not error out
	expectedString := string(b)
	require.Equal(t, expectedString, sampleConfig)
}

func TestInitValidations(t *testing.T) {
	t.Parallel()
	fakeClient := kusto.NewMockClient()
	tests := []struct {
		name          string             // name of the test
		adx           *AzureDataExplorer // the struct to test
		expectedError string             // the error to expect
	}{
		{
			name: "empty_endpoint_configuration",
			adx: &AzureDataExplorer{
				Log:       testutil.Logger{},
				Endpoint:  "",
				Database:  "databasename",
				client:    fakeClient,
				ingesters: make(map[string]ingest.Ingestor),
			},
			expectedError: "endpoint configuration cannot be empty",
		},
		{
			name: "empty_database_configuration",
			adx: &AzureDataExplorer{
				Log:       testutil.Logger{},
				Endpoint:  "endpoint",
				Database:  "",
				client:    fakeClient,
				ingesters: make(map[string]ingest.Ingestor),
			},
			expectedError: "database configuration cannot be empty",
		},
		{
			name: "empty_table_configuration",
			adx: &AzureDataExplorer{
				Log:             testutil.Logger{},
				Endpoint:        "endpoint",
				Database:        "database",
				MetricsGrouping: "SingleTable",
				client:          fakeClient,
				ingesters:       make(map[string]ingest.Ingestor),
			},
			expectedError: "table name cannot be empty for SingleTable metrics grouping type",
		},
		{
			name: "incorrect_metrics_grouping",
			adx: &AzureDataExplorer{
				Log:             testutil.Logger{},
				Endpoint:        "endpoint",
				Database:        "database",
				MetricsGrouping: "MultiTable",
				client:          fakeClient,
				ingesters:       make(map[string]ingest.Ingestor),
			},
			expectedError: "metrics grouping type is not valid",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.adx.Init()
			require.Error(t, err)
			require.Equal(t, tt.expectedError, err.Error())
		})
	}

}

func TestConnect(t *testing.T) {
	fakeClient := kusto.NewMockClient()
	plugin := AzureDataExplorer{
		Log:       testutil.Logger{},
		Endpoint:  "someendpoint",
		Database:  "databasename",
		client:    fakeClient,
		ingesters: make(map[string]ingest.Ingestor),
	}

	connection := plugin.Connect()
	require.Error(t, connection)
	require.Equal(t, "MSI not available", connection.Error())
}

func TestInit(t *testing.T) {
	fakeClient := kusto.NewMockClient()
	plugin := AzureDataExplorer{
		Log:       testutil.Logger{},
		Endpoint:  "someendpoint",
		Database:  "databasename",
		client:    fakeClient,
		ingesters: make(map[string]ingest.Ingestor),
	}
	initResponse := plugin.Init()
	require.Equal(t, initResponse, nil)
}

func TestCreateRealIngestorManaged(t *testing.T) {
	kustoLocalClient := kusto.NewMockClient()
	localIngestor, err := createIngestorByTable(kustoLocalClient, "telegrafdb", "metrics", "managed")
	require.Nil(t, err)
	require.NotNil(t, localIngestor)
}

func TestCreateRealIngestorQueued(t *testing.T) {
	kustoLocalClient := kusto.NewMockClient()
	localIngestor, err := createIngestorByTable(kustoLocalClient, "telegrafdb", "metrics", "queued")
	require.Nil(t, err)
	require.NotNil(t, localIngestor)
}
func TestClose(t *testing.T) {
	fakeClient := kusto.NewMockClient()
	adx := AzureDataExplorer{
		Log:       testutil.Logger{},
		Endpoint:  "someendpoint",
		Database:  "databasename",
		client:    fakeClient,
		ingesters: make(map[string]ingest.Ingestor),
	}
	err := adx.Close()
	require.Nil(t, err)
	// client becomes nil in the end
	require.Nil(t, adx.client)
	require.Nil(t, adx.ingesters)
}

type mockIngestor struct {
	records []string
}

func (m *mockIngestor) FromReader(ctx context.Context, reader io.Reader, options ...ingest.FileOption) (*ingest.Result, error) {
	bufbytes, _ := ioutil.ReadAll(reader)
	metricjson := string(bufbytes)
	m.SetRecords(strings.Split(metricjson, "\n"))
	return &ingest.Result{}, nil
}

func (m *mockIngestor) FromFile(ctx context.Context, fPath string, options ...ingest.FileOption) (*ingest.Result, error) {
	return &ingest.Result{}, nil
}

func (f *mockIngestor) SetRecords(records []string) {
	f.records = records
}

// Name receives a copy of Foo since it doesn't need to modify it.
func (f *mockIngestor) Records() []string {
	return f.records
}

func (m *mockIngestor) Close() error {
	return nil
}
