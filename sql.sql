CREATE TABLE IF NOT EXISTS `geo-roulette.tg_jobs.messages_raw` (
    chat_id STRING,
    user STRING,
    sender_chat_user_name STRING,
    sender_chat_id STRING,
    user_name STRING,
    datetime TIMESTAMP,
    link STRING,
    id INTEGER,
    text STRING,
    empty BOOLEAN,
    date DATE
);

TRUNCATE TABLE `geo-roulette.tg_jobs.messages_raw`;
DROP TABLE IF EXISTS `geo-roulette.tg_jobs.messages_raw`;

CREATE TABLE IF NOT EXISTS `geo-roulette.tg_jobs.upload_msg_status` (
    filename STRING,
    path STRING,
    status STRING,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    created_date DATE,
    size INTEGER,
    `to` INTEGER,
    `from` INTEGER,
    chat_id STRING
);

TRUNCATE TABLE `geo-roulette.tg_jobs.upload_msg_status`;
DROP TABLE IF EXISTS `geo-roulette.tg_jobs.upload_msg_status`;
