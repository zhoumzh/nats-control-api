// Package api provides HTTP handlers for the NATS RBAC management system
//
//	@title			NATS RBAC API
//	@version		1.0
//	@description	API for managing NATS accounts and users with JWT authentication
//	@termsOfService	http://swagger.io/terms/
//
//	@contact.name	API Support
//	@contact.email	support@example.com
//
//	@license.name	MIT
//	@license.url	https://opensource.org/licenses/MIT
//
//	@host		localhost:8080
//	@BasePath	/api/v1
//
//	@schemes	http https
package api

import (
	"net/http"
	"strconv"
	"strings"

	"nats-control-api/internal/service"
	"nats-control-api/pkg/models"

	"github.com/gin-gonic/gin"
)

type AccountHandler struct {
	accountService *service.AccountService
	jwtService     *service.JWTService
	clusterService *service.ClusterService
}

func NewAccountHandler(accountService *service.AccountService, jwtService *service.JWTService, clusterService *service.ClusterService) *AccountHandler {
	return &AccountHandler{
		accountService: accountService,
		jwtService:     jwtService,
		clusterService: clusterService,
	}
}

// CreateAccount godoc
//
//	@Summary		Create a new NATS account
//	@Description	Create a new NATS account with the specified configuration
//	@Tags			accounts
//	@Accept			json
//	@Produce		json
//	@Param			account	body		models.CreateAccountRequest	true	"Account creation request"
//	@Success		201		{object}	models.Account				"Created account"
//	@Failure		400		{object}	map[string]string			"Bad request"
//	@Failure		500		{object}	map[string]string			"Internal server error"
//	@Router			/accounts [post]
func (h *AccountHandler) CreateAccount(c *gin.Context) {
	var req models.CreateAccountRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	account, err := h.accountService.CreateAccount(&req)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusCreated, account)
}

// GetAccount godoc
//
//	@Summary		Get account by ID
//	@Description	Retrieve a NATS account by its ID
//	@Tags			accounts
//	@Produce		json
//	@Param			id	path		string				true	"Account ID"
//	@Success		200	{object}	models.Account		"Account details"
//	@Failure		404	{object}	map[string]string	"Account not found"
//	@Failure		500	{object}	map[string]string	"Internal server error"
//	@Router			/accounts/{id} [get]
func (h *AccountHandler) GetAccount(c *gin.Context) {
	id := c.Param("id")

	account, err := h.accountService.GetAccount(id)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if account == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Account not found"})
		return
	}

	c.JSON(http.StatusOK, account)
}

// UpdateAccount godoc
//
//	@Summary		Update account
//	@Description	Update an existing NATS account
//	@Tags			accounts
//	@Accept			json
//	@Produce		json
//	@Param			id		path		string						true	"Account ID"
//	@Param			account	body		models.UpdateAccountRequest	true	"Account update request"
//	@Success		200		{object}	models.Account				"Updated account"
//	@Failure		400		{object}	map[string]string			"Bad request"
//	@Failure		404		{object}	map[string]string			"Account not found"
//	@Failure		500		{object}	map[string]string			"Internal server error"
//	@Router			/accounts/{id} [put]
func (h *AccountHandler) UpdateAccount(c *gin.Context) {
	id := c.Param("id")

	var req models.UpdateAccountRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	account, err := h.accountService.UpdateAccount(id, &req)
	if err != nil {
		if err.Error() == "account not found" {
			c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, account)
}

// ListAccounts godoc
//
//	@Summary		List accounts
//	@Description	Get a list of NATS accounts with pagination and filters
//	@Tags			accounts
//	@Produce		json
//	@Param			limit		query		int					false	"Number of accounts to return (default 20)"
//	@Param			offset		query		int					false	"Number of accounts to skip (default 0)"
//	@Param			search		query		string				false	"Search in account name and description"
//	@Param			status		query		string				false	"Filter by account status (active/disabled)"
//	@Param			account_type	query		string				false	"Filter by account type (system/normal)"
//	@Param			cluster_id	query		string				false	"Filter by origin cluster ID"
//	@Param			sort_by		query		string				false	"Sort field (name/created_at/updated_at/status)"
//	@Param			order		query		string				false	"Sort order (asc/desc)"
//	@Success		200			{object}	models.AccountListResponse	"List of accounts with pagination"
//	@Failure		400			{object}	map[string]string			"Bad request"
//	@Failure		500			{object}	map[string]string			"Internal server error"
//	@Router			/accounts [get]
func (h *AccountHandler) ListAccounts(c *gin.Context) {
	req := &models.AccountListRequest{
		Limit:  20,
		Offset: 0,
	}

	// Parse pagination parameters
	if l := c.Query("limit"); l != "" {
		if parsed, err := strconv.Atoi(l); err == nil && parsed > 0 {
			req.Limit = parsed
		}
	}

	if o := c.Query("offset"); o != "" {
		if parsed, err := strconv.Atoi(o); err == nil && parsed >= 0 {
			req.Offset = parsed
		}
	}

	// Parse filter parameters
	req.Search = c.Query("search")
	req.Status = c.Query("status")
	req.AccountType = c.Query("account_type")
	req.ClusterID = c.Query("cluster_id")
	req.SortBy = c.Query("sort_by")
	req.Order = c.Query("order")

	// Support legacy page/page_size parameters for compatibility
	if page := c.Query("page"); page != "" {
		if parsed, err := strconv.Atoi(page); err == nil && parsed > 0 {
			req.Offset = (parsed - 1) * req.Limit
		}
	}

	if pageSize := c.Query("page_size"); pageSize != "" {
		if parsed, err := strconv.Atoi(pageSize); err == nil && parsed > 0 {
			req.Limit = parsed
			// Recalculate offset if page was provided
			if page := c.Query("page"); page != "" {
				if parsed, err := strconv.Atoi(page); err == nil && parsed > 0 {
					req.Offset = (parsed - 1) * req.Limit
				}
			}
		}
	}

	response, err := h.accountService.ListAccountsWithFilters(req)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	clusters, err := h.clusterService.ListClusters("")
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	clusterMap := make(map[string]string)
	for _, cluster := range clusters {
		clusterMap[cluster.ID] = cluster.Name
	}

	result := make([]*models.AccountWithCluster, 0, len(response.Data))
	for _, account := range response.Data {
		accountWithCluster := &models.AccountWithCluster{
			Account:     account,
			ClusterName: "--",
		}

		if clusterName, ok := clusterMap[account.OriginClusterID]; ok {
			accountWithCluster.ClusterName = clusterName
		}

		result = append(result, accountWithCluster)
	}

	c.JSON(http.StatusOK, result)
}

// DisableAccount godoc
//
//	@Summary		Disable account
//	@Description	Disable a NATS account (removes JWT from NATS server)
//	@Tags			accounts
//	@Produce		json
//	@Param			id	path		string				true	"Account ID"
//	@Success		200	{object}	map[string]string	"Account disabled successfully"
//	@Failure		404	{object}	map[string]string	"Account not found"
//	@Failure		500	{object}	map[string]string	"Internal server error"
//	@Router			/accounts/{id}/disable [post]
func (h *AccountHandler) DisableAccount(c *gin.Context) {
	id := c.Param("id")

	if err := h.accountService.DisableAccount(id); err != nil {
		if err.Error() == "account not found" {
			c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Account disabled successfully"})
}

// EnableAccount godoc
//
//	@Summary		Enable account
//	@Description	Enable a NATS account (pushes JWT to NATS server)
//	@Tags			accounts
//	@Produce		json
//	@Param			id	path		string				true	"Account ID"
//	@Success		200	{object}	map[string]string	"Account enabled successfully"
//	@Failure		404	{object}	map[string]string	"Account not found"
//	@Failure		500	{object}	map[string]string	"Internal server error"
//	@Router			/accounts/{id}/enable [post]
func (h *AccountHandler) EnableAccount(c *gin.Context) {
	id := c.Param("id")

	if err := h.accountService.EnableAccount(id); err != nil {
		if err.Error() == "account not found" {
			c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Account enabled successfully"})
}

// GetAccountJWT godoc
//
//	@Summary		Get account JWT
//	@Description	Get the JWT token and decoded claims for an account
//	@Tags			accounts
//	@Produce		json
//	@Param			id	path		string					true	"Account ID"
//	@Success		200	{object}	models.JWTResponse		"Account JWT and claims"
//	@Failure		404	{object}	map[string]string		"Account not found"
//	@Failure		500	{object}	map[string]string		"Internal server error"
//	@Router			/accounts/{id}/jwt [get]
func (h *AccountHandler) GetAccountJWT(c *gin.Context) {
	id := c.Param("id")

	account, err := h.accountService.GetAccount(id)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if account == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Account not found"})
		return
	}

	jwtResponse, err := h.jwtService.GetAccountJWT(account.ID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, jwtResponse)
}

// AddAccountExport godoc
//
//	@Summary		Add export to account
//	@Description	Add a subject export to an account for cross-account communication
//	@Tags			accounts
//	@Accept			json
//	@Produce		json
//	@Param			id		path		string						true	"Account ID"
//	@Param			export	body		models.AddExportRequest		true	"Export configuration"
//	@Success		200		{object}	models.Account				"Updated account"
//	@Failure		400		{object}	map[string]string			"Bad request"
//	@Failure		404		{object}	map[string]string			"Account not found"
//	@Failure		500		{object}	map[string]string			"Internal server error"
//	@Router			/accounts/{id}/exports [post]
func (h *AccountHandler) AddAccountExport(c *gin.Context) {
	id := c.Param("id")

	var req models.AddExportRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	account, err := h.accountService.AddAccountExport(id, &req)
	if err != nil {
		if err.Error() == "account not found" {
			c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, account)
}

// AddAccountImport godoc
//
//	@Summary		Add import to account
//	@Description	Add a subject import to an account for cross-account communication
//	@Tags			accounts
//	@Accept			json
//	@Produce		json
//	@Param			id		path		string						true	"Account ID"
//	@Param			import	body		models.AddImportRequest		true	"Import configuration"
//	@Success		200		{object}	models.Account				"Updated account"
//	@Failure		400		{object}	map[string]string			"Bad request"
//	@Failure		404		{object}	map[string]string			"Account not found"
//	@Failure		500		{object}	map[string]string			"Internal server error"
//	@Router			/accounts/{id}/imports [post]
func (h *AccountHandler) AddAccountImport(c *gin.Context) {
	id := c.Param("id")

	var req models.AddImportRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	account, err := h.accountService.AddAccountImport(id, &req)
	if err != nil {
		if err.Error() == "account not found" {
			c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, account)
}

// RemoveAccountExport godoc
//
//	@Summary		Remove export from account
//	@Description	Remove a subject export from an account
//	@Tags			accounts
//	@Produce		json
//	@Param			id		path		string				true	"Account ID"
//	@Param			name	path		string				true	"Export name"
//	@Success		200		{object}	models.Account		"Updated account"
//	@Failure		404		{object}	map[string]string	"Account or export not found"
//	@Failure		500		{object}	map[string]string	"Internal server error"
//	@Router			/accounts/{id}/exports/{name} [delete]
func (h *AccountHandler) RemoveAccountExport(c *gin.Context) {
	id := c.Param("id")
	name := c.Param("name")

	account, err := h.accountService.RemoveAccountExport(id, name)
	if err != nil {
		if err.Error() == "account not found" || err.Error() == "export not found" {
			c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, account)
}

// RemoveAccountImport godoc
//
//	@Summary		Remove import from account
//	@Description	Remove a subject import from an account
//	@Tags			accounts
//	@Produce		json
//	@Param			id		path		string				true	"Account ID"
//	@Param			name	path		string				true	"Import name"
//	@Success		200		{object}	models.Account		"Updated account"
//	@Failure		404		{object}	map[string]string	"Account or import not found"
//	@Failure		500		{object}	map[string]string	"Internal server error"
//	@Router			/accounts/{id}/imports/{name} [delete]
func (h *AccountHandler) RemoveAccountImport(c *gin.Context) {
	id := c.Param("id")
	name := c.Param("name")

	account, err := h.accountService.RemoveAccountImport(id, name)
	if err != nil {
		if err.Error() == "account not found" || err.Error() == "import not found" {
			c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, account)
}

// CreateAccountAssociation godoc
//
//	@Summary		Create cross-account association
//	@Description	Create bidirectional communication between two accounts
//	@Tags			accounts
//	@Accept			json
//	@Produce		json
//	@Param			id			path		string						true	"Source Account ID"
//	@Param			association	body		models.AccountAssociationRequest	true	"Association configuration"
//	@Success		200			{object}	map[string]string			"Association created successfully"
//	@Failure		400			{object}	map[string]string			"Bad request"
//	@Failure		404			{object}	map[string]string			"Account not found"
//	@Failure		500			{object}	map[string]string			"Internal server error"
//	@Router			/accounts/{id}/associate [post]
func (h *AccountHandler) CreateAccountAssociation(c *gin.Context) {
	sourceAccountID := c.Param("id")

	var req models.AccountAssociationRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if err := h.accountService.CreateAccountAssociation(sourceAccountID, &req); err != nil {
		if err.Error() == "account not found" {
			c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Account association created successfully"})
}

// DeleteAccount godoc
//
//	@Summary		Delete account
//	@Description	Delete a NATS account and remove it from the system
//	@Tags			accounts
//	@Produce		json
//	@Param			id	path		string				true	"Account ID"
//	@Success		200	{object}	map[string]string	"Account deleted successfully"
//	@Failure		400	{object}	map[string]string	"Cannot delete account with associated users"
//	@Failure		404	{object}	map[string]string	"Account not found"
//	@Failure		500	{object}	map[string]string	"Internal server error"
//	@Router			/accounts/{id} [delete]
func (h *AccountHandler) DeleteAccount(c *gin.Context) {
	id := c.Param("id")

	if err := h.accountService.DeleteAccount(id); err != nil {
		if err.Error() == "账户未找到" {
			c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
			return
		}
		if strings.Contains(err.Error(), "关联用户") {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Account deleted successfully"})
}

// ManualSyncAccountJWT godoc
//
//	@Summary		手动同步账户JWT到指定集群
//	@Description	手动将账户的JWT同步到选定的集群，通过异步任务处理
//	@Tags			accounts
//	@Accept			json
//	@Produce		json
//	@Param			id		path		string						true	"账户ID"
//	@Param			sync	body		models.ManualSyncJWTRequest	true	"同步请求"
//	@Success		200		{object}	map[string]string			"同步任务创建成功"
//	@Failure		400		{object}	map[string]string			"请求参数错误"
//	@Failure		404		{object}	map[string]string			"账户未找到"
//	@Failure		500		{object}	map[string]string			"服务器内部错误"
//	@Router			/accounts/{id}/sync [post]
func (h *AccountHandler) ManualSyncAccountJWT(c *gin.Context) {
	id := c.Param("id")

	var req models.ManualSyncJWTRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "请求参数错误: " + err.Error()})
		return
	}

	// 验证账户是否存在
	account, err := h.accountService.GetAccount(id)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "获取账户失败: " + err.Error()})
		return
	}
	if account == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "账户未找到"})
		return
	}

	// 创建手动同步任务
	if err := h.accountService.CreateManualSyncJWTTask(id, req.ClusterIDs); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "创建同步任务失败: " + err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"message":       "JWT同步任务已创建，将异步处理",
		"account_id":    id,
		"cluster_count": len(req.ClusterIDs),
	})
}

// GetAccountUserCount godoc
//
//	@Summary		获取账号关联的用户数
//	@Description	通过账号ID获取关联的用户数
//	@Tags			accounts
//	@Produce		json
//	@Param			id	path		string	true	"账号ID"
//	@Success		200	{number}	int64	"用户数"
//	@Failure		500	{object}	map[string]string	"服务器内部错误"
//	@Router			/accounts/{id}/user-count [get]
func (h *AccountHandler) GetAccountUserCount(c *gin.Context) {
	id := c.Param("id")

	count, err := h.accountService.GetUserCountByAccountID(id)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, count)
}

// GetAccountJWTTaskCount godoc
//
//	@Summary		获取账号公钥关联的JWT任务数
//	@Description	通过账号公钥获取关联的JWT任务数
//	@Tags			accounts
//	@Produce		json
//	@Param			publicKey	path		string	true	"账号公钥"
//	@Success		200			{number}	int64	"JWT任务数"
//	@Failure		500			{object}	map[string]string	"服务器内部错误"
//	@Router			/accounts/public-key/{publicKey}/jwt-task-count [get]
func (h *AccountHandler) GetAccountJWTTaskCount(c *gin.Context) {
	publicKey := c.Param("publicKey")

	count, err := h.accountService.GetJWTTaskCountByAccountPublicKey(publicKey)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, count)
}
