package main

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gocql/gocql"
	"github.com/google/uuid"
)

type Todo struct {
    ID          uuid.UUID `json:"id"`
    UserID      uuid.UUID `json:"user_id"`
    Title       string    `json:"title"`
    Description string    `json:"description"`
    Status      string    `json:"status"`
    Created     time.Time `json:"created"`
    Updated     time.Time `json:"updated"`
}

func main() {
    r := gin.Default()

    r.POST("/todos", createTodo)
    r.GET("/todos/:id", getTodo)
    r.PUT("/todos/:id", updateTodo)
    r.DELETE("/todos/:id", deleteTodo)
    r.GET("/todos", listTodos)

    r.Run()
}
// Function to interpolate query with parameters
func interpolateParams(query string, params []interface{}) string {
    // Replace placeholders with parameters
    for _, param := range params {
        query = strings.Replace(query, "?", fmt.Sprintf("'%v'", param), 1)
    }
    return query

}
func createTodo(c *gin.Context) {
	fmt.Println("create Todo")

	var todo Todo
	if err := c.BindJSON(&todo); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	fmt.Println(todo.UserID)

	// Validate required fields
	if todo.Title == "" || todo.Description == "" || todo.UserID.String() == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Title, UserId and Description are required"})
		return
	}


	todo.ID = uuid.New()
	fmt.Println(todo.UserID, todo.Description, todo.Title)
	todo.Created = time.Now()
	todo.Updated = time.Now()

	session := GetSession()
	if session == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get database session"})
		return
	}

	// Convert UUIDs to strings before insertion
	err := session.Query("INSERT INTO todos (id, user_id, title, description, status, created, updated) VALUES (?, ?, ?, ?, ?, ?, ?)",
		todo.ID.String(), todo.UserID.String(), todo.Title, todo.Description, todo.Status, todo.Created, todo.Updated).Exec()

	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, todo)
}

func listTodos(c *gin.Context) {
    userID := c.Query("user_id")
    status := c.Query("status")
    limit := c.DefaultQuery("limit", "10")
	

    var todos []Todo
    session := GetSession()

    query := "SELECT id, user_id, title, description, status, created, updated FROM todos"
    var queryParams []interface{}

    if userID != "" {
        query += " WHERE user_id = ?"
        queryParams = append(queryParams, userID)
    }

    if status != "" {
        if userID == "" {
            query += " WHERE"
        } else {
            query += " AND"
        }
        query += " status = ?"
        queryParams = append(queryParams, status)
    }

    query += " LIMIT ?"
    limitInt, err := strconv.Atoi(limit)
    if err != nil {
        c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid limit format"})
        return
    }
    queryParams = append(queryParams, limitInt)
	// query += " OFFSET 1"
    // limitOffset, err := strconv.Atoi("1")
    // if err != nil {
    //     c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid limit format"})
    //     return
    // }
    // queryParams = append(queryParams, limitOffset)

    query += " ALLOW FILTERING"
    fmt.Println(query,interpolateParams(query,queryParams))
	

    iter := session.Query(query, queryParams...).Iter()
    for {
        var todo Todo
        var id gocql.UUID
        var userID gocql.UUID
        if !iter.Scan(&id, &userID, &todo.Title, &todo.Description, &todo.Status, &todo.Created, &todo.Updated) {
            break
        }
        // Convert gocql.UUID to uuid.UUID
        todo.ID = uuid.UUID(id)
        todo.UserID = uuid.UUID(userID)
        todos = append(todos, todo)
    }

    if err := iter.Close(); err != nil {
        c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
        return
    }

    c.JSON(http.StatusOK, todos)
}

func getTodo(c *gin.Context) {
    id := c.Param("id")
	fmt.Println(id)

    var todos []Todo
    session := GetSession()
    iter := session.Query("SELECT id, user_id, title, description, status, created, updated FROM todos WHERE id = ?", id).Iter()
    for {
        var todo Todo
        var id gocql.UUID
        var userID gocql.UUID
        if !iter.Scan(&id, &userID, &todo.Title, &todo.Description, &todo.Status, &todo.Created, &todo.Updated) {
            break
        }
        // Convert gocql.UUID to uuid.UUID
        todo.ID = uuid.UUID(id)
        todo.UserID = uuid.UUID(userID)
        todos = append(todos, todo)
    }

    if err := iter.Close(); err != nil {
        c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
        return
    }


    c.JSON(http.StatusOK, todos[0])
}

func updateTodo(c *gin.Context) {
    id := c.Param("id")

    // Check if ID is a valid UUID
    todoID, err := gocql.ParseUUID(id)
    if err != nil {
        c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid todo ID"})
        return
    }

    var todo Todo
    if err := c.BindJSON(&todo); err != nil {
        c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
        return
    }

    // Check if JSON body is empty or all fields are empty
    if todo.Title == "" && todo.Description == "" && todo.Status == "" {
        c.JSON(http.StatusBadRequest, gin.H{"error": "Empty update request"})
        return
    }

    // Set updated timestamp
    todo.Updated = time.Now()

    session := GetSession()

    // Build the query dynamically, excluding id and user_id fields
    query := "UPDATE todos SET"
    var params []interface{}

    if todo.Title != "" {
		fmt.Println("title updated",todo.Title)
        query += " title = ?,"
        params = append(params, todo.Title)
    }
    if todo.Description != "" {
		fmt.Println("description updated",todo.Title)
        query += " description = ?,"
        params = append(params, todo.Description)
    }
    if todo.Status != "" {
        query += " status = ?,"
        params = append(params, todo.Status)
    }

    // Add updated timestamp and WHERE clause
    query += " updated = ? WHERE id = ?"
    params = append(params, todo.Updated, todoID)

    // Execute the query with dynamic params
	fmt.Println(query,params,"dev")
	queryWithParams := interpolateParams(query, params)

    // Print the query with parameters included
    fmt.Println("Executing query:", queryWithParams)
    err = session.Query(query, params...).Exec()
    if err != nil {
        c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
        return
    }

    fmt.Println("todo",todo)
	result := map[string]interface{}{
		"success" : true,
		"status" : 200,
		"message": "Todo Updated",
	}
	c.JSON(http.StatusOK,result)
}
func deleteTodo(c *gin.Context) {
    id := c.Param("id")

    session := GetSession()
    err := session.Query("DELETE FROM todos WHERE id = ?", id).Exec()

    if err != nil {
        c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
        return
    }

    c.JSON(http.StatusOK, gin.H{"message": "Todo deleted"})
}
