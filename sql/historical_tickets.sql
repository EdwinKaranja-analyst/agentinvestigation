-- Historical Tickets Query
-- Get previous tickets for this account

DECLARE @IMEI VARCHAR(50) = ?
DECLARE @AccountNumber VARCHAR(50) = ?

SELECT TOP 10
    TicketId,
    Subject,
    Status,
    CreatedTime,
    ReasonForInteraction
FROM [base_freshdesk].[base_freshdesk_api_tickets] WITH (NOLOCK)
WHERE (DeviceIMEI = @IMEI OR AccountNumber = @AccountNumber)
ORDER BY CreatedTime DESC
