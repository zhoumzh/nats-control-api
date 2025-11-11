package api

import (
	"net/http"
	"strconv"

	"nats-control-api/internal/service"
	"nats-control-api/pkg/models"
	
	"github.com/gin-gonic/gin"
)

type UserHandler struct {
	userService *service.UserService
	jwtService  *service.JWTService
}

func NewUserHandler(userService *service.UserService, jwtService *service.JWTService) *UserHandler {
	return &UserHandler{
		userService: userService,
		jwtService:  jwtService,
	}
}

// CreateUser godoc
//	@Summary		Create a new user in an account
//	@Description	Create a new NATS user within the specified account
//	@Tags			users
//	@Accept			json
//	@Produce		json
//	@Param			id		path		string					true	"Account ID"
//	@Param			user		body		models.CreateUserRequest	true	"User creation request"
//	@Success		201			{object}	models.User				"Created user"
//	@Failure		400			{object}	map[string]string		"Bad request"
//	@Failure		404			{object}	map[string]string		"Account not found"
//	@Failure		500			{object}	map[string]string		"Internal server error"
//	@Router			/accounts/{id}/users [post]
func (h *UserHandler) CreateUser(c *gin.Context) {
	accountID := c.Param("id")
	
	var req models.CreateUserRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	
	user, err := h.userService.CreateUser(accountID, &req)
	if err != nil {
		if err.Error() == "account not found" || err.Error() == "cannot create user in disabled account" {
			c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	
	c.JSON(http.StatusCreated, user)
}

// GetUser godoc
//	@Summary		Get user by ID
//	@Description	Retrieve a NATS user by its ID
//	@Tags			users
//	@Produce		json
//	@Param			id	path		string				true	"User ID"
//	@Success		200	{object}	models.User			"User details"
//	@Failure		404	{object}	map[string]string	"User not found"
//	@Failure		500	{object}	map[string]string	"Internal server error"
//	@Router			/users/{id} [get]
func (h *UserHandler) GetUser(c *gin.Context) {
	id := c.Param("id")
	
	user, err := h.userService.GetUser(id)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if user == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "User not found"})
		return
	}
	
	c.JSON(http.StatusOK, user)
}

// UpdateUser godoc
//	@Summary		Update user
//	@Description	Update an existing NATS user
//	@Tags			users
//	@Accept			json
//	@Produce		json
//	@Param			id		path		string					true	"User ID"
//	@Param			user	body		models.UpdateUserRequest	true	"User update request"
//	@Success		200		{object}	models.User				"Updated user"
//	@Failure		400		{object}	map[string]string		"Bad request"
//	@Failure		404		{object}	map[string]string		"User not found"
//	@Failure		500		{object}	map[string]string		"Internal server error"
//	@Router			/users/{id} [put]
func (h *UserHandler) UpdateUser(c *gin.Context) {
	id := c.Param("id")
	
	var req models.UpdateUserRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	
	user, err := h.userService.UpdateUser(id, &req)
	if err != nil {
		if err.Error() == "user not found" {
			c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	
	c.JSON(http.StatusOK, user)
}

// ListUsers godoc
//	@Summary		List users in an account
//	@Description	Get a list of NATS users within the specified account with pagination
//	@Tags			users
//	@Produce		json
//	@Param			id	path		string				true	"Account ID"
//	@Param			limit		query		int					false	"Number of users to return (default 20)"
//	@Param			offset		query		int					false	"Number of users to skip (default 0)"
//	@Success		200			{array}		models.User			"List of users"
//	@Failure		400			{object}	map[string]string	"Bad request"
//	@Failure		500			{object}	map[string]string	"Internal server error"
//	@Router			/accounts/{id}/users [get]
func (h *UserHandler) ListUsers(c *gin.Context) {
	accountID := c.Param("id")
	
	limit := 20
	offset := 0
	
	if l := c.Query("limit"); l != "" {
		if parsed, err := strconv.Atoi(l); err == nil && parsed > 0 {
			limit = parsed
		}
	}
	
	if o := c.Query("offset"); o != "" {
		if parsed, err := strconv.Atoi(o); err == nil && parsed >= 0 {
			offset = parsed
		}
	}
	
	users, err := h.userService.ListUsers(accountID, limit, offset)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	
	c.JSON(http.StatusOK, users)
}

// GetAllUsers godoc
//	@Summary		Get all users with filtering
//	@Description	Get all users across accounts with optional filtering by account ID, status, limit and offset
//	@Tags			users
//	@Produce		json
//	@Param			account_id	query		string				false	"Account ID (required)"
//	@Param			status		query		string				false	"User status (active/disabled)"
//	@Param			limit		query		int					false	"Number of users to return (default 20)"
//	@Param			offset		query		int					false	"Number of users to skip (default 0)"
//	@Success		200			{array}		models.User			"List of users"
//	@Failure		400			{object}	map[string]string	"Bad request"
//	@Failure		500			{object}	map[string]string	"Internal server error"
//	@Router			/users [get]
func (h *UserHandler) GetAllUsers(c *gin.Context) {
	accountID := c.Query("account_id")
	status := c.Query("status")
	
	limit := 20
	offset := 0
	
	if l := c.Query("limit"); l != "" {
		if parsed, err := strconv.Atoi(l); err == nil && parsed > 0 {
			limit = parsed
		}
	}
	
	if o := c.Query("offset"); o != "" {
		if parsed, err := strconv.Atoi(o); err == nil && parsed >= 0 {
			offset = parsed
		}
	}
	
	users, err := h.userService.GetAllUsers(accountID, status, limit, offset)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	
	c.JSON(http.StatusOK, users)
}

// ListAdminUsersFromNonSystemAccounts godoc
//	@Summary		List admin users from non-system accounts
//	@Description	Get a list of admin users from all non-system accounts
//	@Tags			users
//	@Produce		json
//	@Success		200			{array}		models.SimplifiedUserResponse	"List of admin users"
//	@Failure		500			{object}	map[string]string				"Internal server error"
//	@Router			/users/admin/non-system [get]
func (h *UserHandler) ListAdminUsersFromNonSystemAccounts(c *gin.Context) {
	users, err := h.userService.ListAdminUsersFromNonSystemAccounts()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	
	// 转换为简化的响应结构
	simplifiedUsers := make([]*models.SimplifiedUserResponse, len(users))
	for i, user := range users {
		simplifiedUsers[i] = &models.SimplifiedUserResponse{
			ID:        user.ID,
			Name:      user.Name,
			AccountID: user.AccountID,
		}
	}
	
	c.JSON(http.StatusOK, simplifiedUsers)
}

// DisableUser godoc
//	@Summary		Disable user
//	@Description	Disable a NATS user (removes JWT from NATS server)
//	@Tags			users
//	@Produce		json
//	@Param			id	path		string				true	"User ID"
//	@Success		200	{object}	map[string]string	"User disabled successfully"
//	@Failure		404	{object}	map[string]string	"User not found"
//	@Failure		500	{object}	map[string]string	"Internal server error"
//	@Router			/users/{id}/disable [post]
func (h *UserHandler) DisableUser(c *gin.Context) {
	id := c.Param("id")
	
	if err := h.userService.DisableUser(id); err != nil {
		if err.Error() == "user not found" {
			c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	
	c.JSON(http.StatusOK, gin.H{"message": "User disabled successfully"})
}

// EnableUser godoc
//	@Summary		Enable user
//	@Description	Enable a NATS user (pushes JWT to NATS server)
//	@Tags			users
//	@Produce		json
//	@Param			id	path		string				true	"User ID"
//	@Success		200	{object}	map[string]string	"User enabled successfully"
//	@Failure		404	{object}	map[string]string	"User not found"
//	@Failure		500	{object}	map[string]string	"Internal server error"
//	@Router			/users/{id}/enable [post]
func (h *UserHandler) EnableUser(c *gin.Context) {
	id := c.Param("id")
	
	if err := h.userService.EnableUser(id); err != nil {
		if err.Error() == "user not found" {
			c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	
	c.JSON(http.StatusOK, gin.H{"message": "User enabled successfully"})
}

// GetUserJWT godoc
//	@Summary		Get user JWT
//	@Description	Get the JWT token and decoded claims for a user
//	@Tags			users
//	@Produce		json
//	@Param			id	path		string					true	"User ID"
//	@Success		200	{object}	models.JWTResponse		"User JWT and claims"
//	@Failure		404	{object}	map[string]string		"User not found"
//	@Failure		500	{object}	map[string]string		"Internal server error"
//	@Router			/users/{id}/jwt [get]
func (h *UserHandler) GetUserJWT(c *gin.Context) {
	id := c.Param("id")
	
	user, err := h.userService.GetUser(id)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if user == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "User not found"})
		return
	}
	
	jwtToken, err := h.jwtService.GetUserJWT(user)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	
	c.JSON(http.StatusOK, gin.H{
		"jwt": jwtToken,
		"user_id": user.ID,
		"account_id": user.AccountID,
	})
}

// GetUserCreds godoc
//	@Summary		Get user creds file content
//	@Description	Get the creds file content for a user
//	@Tags			users
//	@Produce		json
//	@Param			id	path		string					true	"User ID"
//	@Success		200	{object}	map[string]string		"User creds file content"
//	@Failure		404	{object}	map[string]string		"User not found"
//	@Failure		500	{object}	map[string]string		"Internal server error"
//	@Router			/users/{id}/creds [get]
func (h *UserHandler) GetUserCreds(c *gin.Context) {
	id := c.Param("id")
	
	user, err := h.userService.GetUser(id)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if user == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "User not found"})
		return
	}
	
	// Generate creds content if not exists
	if user.CredsFile == nil || *user.CredsFile == "" {
		credsContent, err := h.jwtService.GenerateUserCredsFile(user)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to generate creds file: " + err.Error()})
			return
		}
		
		// Save the generated creds file content to database
		err = h.userService.UpdateUserCredsFile(id, credsContent)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to save creds file: " + err.Error()})
			return
		}
		
		c.JSON(http.StatusOK, gin.H{"creds_content": credsContent})
		return
	}
	
	c.JSON(http.StatusOK, gin.H{"creds_content": *user.CredsFile})
}

// DownloadUserCreds godoc
//	@Summary		Download user creds file
//	@Description	Download the creds file for a user as a file attachment
//	@Tags			users
//	@Produce		application/octet-stream
//	@Param			id	path		string					true	"User ID"
//	@Success		200	{file}		file					"User creds file"
//	@Failure		404	{object}	map[string]string		"User not found"
//	@Failure		500	{object}	map[string]string		"Internal server error"
//	@Router			/users/{id}/creds/download [get]
func (h *UserHandler) DownloadUserCreds(c *gin.Context) {
	id := c.Param("id")
	
	user, err := h.userService.GetUser(id)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if user == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "User not found"})
		return
	}
	
	var credsContent string
	
	// Generate creds content if not exists
	if user.CredsFile == nil || *user.CredsFile == "" {
		content, err := h.jwtService.GenerateUserCredsFile(user)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to generate creds file: " + err.Error()})
			return
		}
		
		// Save the generated creds file content to database
		err = h.userService.UpdateUserCredsFile(id, content)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to save creds file: " + err.Error()})
			return
		}
		
		credsContent = content
	} else {
		credsContent = *user.CredsFile
	}
	
	// Set headers for file download
	c.Header("Content-Type", "application/octet-stream")
	c.Header("Content-Disposition", "attachment; filename=\""+user.Name+".creds\"")
	c.Header("Content-Length", string(len(credsContent)))
	
	c.String(http.StatusOK, credsContent)
}

// DeleteUser godoc
//	@Summary		Delete user
//	@Description	Delete a NATS user and remove it from the system
//	@Tags			users
//	@Produce		json
//	@Param			id	path		string				true	"User ID"
//	@Success		200	{object}	map[string]string	"User deleted successfully"
//	@Failure		404	{object}	map[string]string	"User not found"
//	@Failure		500	{object}	map[string]string	"Internal server error"
//	@Router			/users/{id} [delete]
func (h *UserHandler) DeleteUser(c *gin.Context) {
	id := c.Param("id")
	
	if err := h.userService.DeleteUser(id); err != nil {
		if err.Error() == "用户未找到" {
			c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	
	c.JSON(http.StatusOK, gin.H{"message": "User deleted successfully"})
}

// CopyUserContext godoc
//	@Summary		Get copy context commands for user
//	@Description	Generate commands to copy user context environment
//	@Tags			users
//	@Produce		plain
//	@Param			id	path		string				true	"User ID"
//	@Success		200	{string}	string				"Copy context commands"
//	@Failure		404	{object}	map[string]string	"User not found"
//	@Failure		500	{object}	map[string]string	"Internal server error"
//	@Router			/users/{id}/copy-context [get]
func (h *UserHandler) CopyUserContext(c *gin.Context) {
	id := c.Param("id")
	
	commands, err := h.userService.GetCopyContextCommands(id)
	if err != nil {
		if err.Error() == "user not found" {
			c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	
	c.Header("Content-Type", "text/plain")
	c.String(http.StatusOK, commands)
}