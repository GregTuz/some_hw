const readerUsername = process.env.MONGO_READER_USERNAME;
const readerPassword = process.env.MONGO_READER_PASSWORD;

const adminUsername = process.env.MONGO_INITDB_ROOT_USERNAME;
const adminPassword = process.env.MONGO_INITDB_ROOT_PASSWORD;

const dbName = process.env.MONGO_DB_NAME;

db = db.getSiblingDB(dbName);

const collections = [
    "UserSessions",
    "ProductPriceHistory",
    "EventLogs",
    "SupportTickets",
    "UserRecommendations",
    "ModerationQueue",
    "SearchQueries"
];

collections.forEach(collection => {
    db.createCollection(collection);
});

db.createUser({
    user: adminUsername,
    pwd: adminPassword,
    roles: [
        { role: "root", db: "admin"},
        { role: "dbAdmin", db: dbName }
    ]
});

db.createUser({
    user: readerUsername,
    pwd: readerPassword,
    roles: [
        { role: "read", db: dbName }
    ]
});
