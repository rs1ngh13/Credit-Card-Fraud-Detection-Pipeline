CREATE TABLE FraudFlags (
    transaction_id VARCHAR(100),
    user_id VARCHAR(100),
    timestamp DATETIME,
    amount FLOAT,
    merchant_id VARCHAR(100),
    location VARCHAR(100),
    risk_score FLOAT,
    anomaly_flag BIT
);