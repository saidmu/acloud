package dynamodb

import (
	"encoding/json"
	"fmt"
	"log"
	"math"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
)

// Payload interface
type Payload interface {
	Payload() (map[string]*dynamodb.AttributeValue, error)
}

// Payloads interface
type Payloads interface {
	Payloads() ([]map[string]*dynamodb.AttributeValue, error)
}

// WriteRecord func writes only one record at a time
// data: Payload interface
// table: DynamoDB table name
func WriteRecord(client *dynamodb.DynamoDB, data Payload, table string) error {
	item, err := data.Payload()
	if err != nil {
		return err
	}
	input := &dynamodb.PutItemInput{Item: item, TableName: aws.String(table)}
	_, err = client.PutItem(input)
	if err != nil {
		return err
	}
	return nil
}

// WriteRecords func writes a bunch of record into DynamoDB
func WriteRecords(client *dynamodb.DynamoDB, data []map[string]*dynamodb.AttributeValue, table string) error {
	length := int(math.Ceil(float64(len(data)) / float64(25)))
	for i := 0; i < length; i++ {
		if i < length-1 {
			var temp []*dynamodb.WriteRequest
			for _, v := range data[i*25 : (i+1)*25] {
				temp = append(temp, &dynamodb.WriteRequest{PutRequest: &dynamodb.PutRequest{Item: v}})
			}
			input := &dynamodb.BatchWriteItemInput{RequestItems: map[string][]*dynamodb.WriteRequest{table: temp}}
			_, err := client.BatchWriteItem(input)
			if err != nil {
				return err
			}
		} else {
			var temp []*dynamodb.WriteRequest
			for _, v := range data[i*25:] {
				temp = append(temp, &dynamodb.WriteRequest{PutRequest: &dynamodb.PutRequest{Item: v}})
			}
			input := &dynamodb.BatchWriteItemInput{RequestItems: map[string][]*dynamodb.WriteRequest{table: temp}}
			_, err := client.BatchWriteItem(input)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// QueryRecords will return a list of records according to a specific condition
// table: DynamoDB table name
// index: DynamoDB index name
// key: DynamoDB key name
// value: DynamoDB value of key
func QueryRecords(client *dynamodb.DynamoDB, table, index, key, value string, condition expression.ConditionBuilder) ([]map[string]*dynamodb.AttributeValue, error) {
	keyCondition := expression.Key(key).Equal(expression.Value(value))
	expr, err := expression.NewBuilder().WithFilter(condition).WithKeyCondition(keyCondition).Build()
	if err != nil {
		return nil, err
	}
	input := &dynamodb.QueryInput{
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		KeyConditionExpression:    expr.KeyCondition(),
		FilterExpression:          expr.Filter(),
		IndexName:                 aws.String(index),
		TableName:                 aws.String(table),
	}
	var output []map[string]*dynamodb.AttributeValue
	for {
		result, err := client.Query(input)
		if err != nil {
			return nil, err
		}
		output = append(output, result.Items...)
		if result.LastEvaluatedKey == nil {
			break
		}
		input.ExclusiveStartKey = result.LastEvaluatedKey
	}
	return output, nil
}

// QueryRecordsWithFilter func
func QueryRecordWithFilter(client *dynamodb.DynamoDB, table string, condition expression.KeyConditionBuilder, filter expression.ConditionBuilder) ([]map[string]*dynamodb.AttributeValue, error) {
	expr, err := expression.NewBuilder().WithKeyCondition(condition).WithFilter(filter).Build()
	if err != nil {
		return nil, err
	}
	var output []map[string]*dynamodb.AttributeValue
	input := &dynamodb.QueryInput{
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		KeyConditionExpression:    expr.KeyCondition(),
		FilterExpression:          expr.Filter(),
		TableName:                 aws.String(table),
	}
	for {
		result, err := client.Query(input)
		if err != nil {
			return nil, err
		}
		output = append(output, result.Items...)
		if result.LastEvaluatedKey == nil {
			break
		}
		input.ExclusiveStartKey = result.LastEvaluatedKey
	}
	return output, nil
}

// AddNumber func
func AddNumber(client *dynamodb.DynamoDB, table string, key map[string]*dynamodb.AttributeValue, name string, number int64) error {
	update := expression.Add(expression.Name(name), expression.Value(number))
	expr, err := expression.NewBuilder().WithUpdate(update).Build()
	if err != nil {
		return err
	}
	input := &dynamodb.UpdateItemInput{
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		Key:                       key,
		TableName:                 aws.String(table),
		UpdateExpression:          expr.Update(),
	}
	_, err = client.UpdateItem(input)
	if err != nil {
		return err
	}
	return nil
}

// PrettyStructPrint  func
func PrettyStructPrint(data interface{}) {
	marshalData, err := json.Marshal(data)
	if err != nil {
		log.Println(err.Error())
	}
	fmt.Println(string(marshalData))
}
