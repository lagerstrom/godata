package opensearch

import (
	"net/url"
	"reflect"

	"github.com/pkg/errors"

	"github.com/lagerstrom/godata/parser"
)

// ErrInvalidInput Client errors
var ErrInvalidInput = errors.New("odata syntax error")

// ODataQuery creates an OpenSearch query based on odata parameters
func ODataQuery(query url.Values) (map[string]interface{}, error) {
	//fmt.Println("ODataQuery", query)
	var filterObj map[string]interface{}

	// Parse url values
	queryMap, err := parser.ParseURLValues(query)
	if err != nil {
		return nil, errors.Wrap(ErrInvalidInput, err.Error())
	}

	limit, _ := queryMap[parser.Top].(int)
	skip, _ := queryMap[parser.Skip].(int)

	filterObj = make(map[string]interface{})
	if queryMap[parser.Filter] != nil {
		filterQuery, _ := queryMap[parser.Filter].(*parser.ParseNode)
		var err error
		filterObj, err = applyFilter(filterQuery)
		if err != nil {
			return nil, errors.Wrap(ErrInvalidInput, err.Error())
		}
	}

	// Prepare Select
	selectFields := []string{}
	if queryMap["$select"] != nil {
		selectSlice := reflect.ValueOf(queryMap["$select"])
		if selectSlice.Len() > 1 && selectSlice.Index(0).Interface().(string) != "*" {
			for i := 0; i < selectSlice.Len(); i++ {
				fieldName := selectSlice.Index(i).Interface().(string)
				selectFields = append(selectFields, fieldName)
			}
		}
	}

	// Sort
	sortFields := []map[string]string{}
	if queryMap[parser.OrderBy] != nil {
		orderBySlice := queryMap[parser.OrderBy].([]parser.OrderItem)
		for _, item := range orderBySlice {
			order := "asc"
			if item.Order == "desc" {
				order = "desc"
			}
			sortFields = append(sortFields, map[string]string{item.Field: order})
		}
	}

	// Build the final OpenSearch query
	opensearchQuery := map[string]interface{}{
		"from":    skip,
		"size":    limit,
		"query":   filterObj,
		"_source": selectFields,
		"sort":    sortFields,
	}

	return opensearchQuery, nil
}

// applyFilter converts OData filter to OpenSearch DSL
func applyFilter(node *parser.ParseNode) (map[string]interface{}, error) {
	filter := make(map[string]interface{})

	if _, ok := node.Token.Value.(string); ok {
		switch node.Token.Value {
		case "eq":
			filter["term"] = map[string]interface{}{node.Children[0].Token.Value.(string): node.Children[1].Token.CleanStringValue()}

		case "ne":
			filter["bool"] = map[string]interface{}{
				"must_not": map[string]interface{}{
					"term": map[string]interface{}{node.Children[0].Token.Value.(string): node.Children[1].Token.CleanStringValue()},
				},
			}

		case "gt":
			filter["range"] = map[string]interface{}{node.Children[0].Token.Value.(string): map[string]interface{}{"gt": node.Children[1].Token.CleanStringValue()}}

		case "ge":
			filter["range"] = map[string]interface{}{node.Children[0].Token.Value.(string): map[string]interface{}{"gte": node.Children[1].Token.CleanStringValue()}}

		case "lt":
			filter["range"] = map[string]interface{}{node.Children[0].Token.Value.(string): map[string]interface{}{"lt": node.Children[1].Token.CleanStringValue()}}

		case "le":
			filter["range"] = map[string]interface{}{node.Children[0].Token.Value.(string): map[string]interface{}{"lte": node.Children[1].Token.CleanStringValue()}}

		case "and":
			leftFilter, err := applyFilter(node.Children[0])
			if err != nil {
				return nil, err
			}
			rightFilter, err := applyFilter(node.Children[1])
			if err != nil {
				return nil, err
			}
			filter["bool"] = map[string]interface{}{
				"must": []map[string]interface{}{leftFilter, rightFilter},
			}

		case "or":
			leftFilter, err := applyFilter(node.Children[0])
			if err != nil {
				return nil, err
			}
			rightFilter, err := applyFilter(node.Children[1])
			if err != nil {
				return nil, err
			}
			filter["bool"] = map[string]interface{}{
				"should": []map[string]interface{}{leftFilter, rightFilter},
			}

		case "startswith":
			filter["prefix"] = map[string]interface{}{node.Children[0].Token.Value.(string): node.Children[1].Token.CleanStringValue()}

		case "contains":
			if node.Children[1].Token.CleanStringValue() == "" {
				filter["wildcard"] = map[string]interface{}{node.Children[0].Token.Value.(string): "*"}
			} else {
				filter["wildcard"] = map[string]interface{}{node.Children[0].Token.Value.(string): "*" + node.Children[1].Token.CleanStringValue().(string) + "*"}
			}

		case "endswith":
			filter["wildcard"] = map[string]interface{}{node.Children[0].Token.Value.(string): "*" + node.Children[1].Token.CleanStringValue().(string)}
		}
	}
	return filter, nil
}
