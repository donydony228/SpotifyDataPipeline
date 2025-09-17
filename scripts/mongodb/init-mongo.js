// 美國求職市場資料工程專案 - MongoDB 初始化腳本

print('🚀 Initializing US Job Data Engineering MongoDB...');

// 切換到我們的專案資料庫
db = db.getSiblingDB('job_market_data');

// 建立集合並插入測試資料
print('📝 Creating collections...');

// 1. 原始爬蟲資料集合
db.createCollection('raw_jobs_data', {
    validator: {
        $jsonSchema: {
            bsonType: "object",
            required: ["source", "job_data", "metadata"],
            properties: {
                source: {
                    bsonType: "string",
                    enum: ["linkedin", "indeed", "glassdoor", "angellist"]
                },
                job_data: {
                    bsonType: "object"
                },
                metadata: {
                    bsonType: "object",
                    required: ["scraped_at", "batch_id"],
                    properties: {
                        scraped_at: { bsonType: "date" },
                        batch_id: { bsonType: "string" },
                        scraper_version: { bsonType: "string" },
                        source_url: { bsonType: "string" }
                    }
                }
            }
        }
    }
});

// 2. 資料品質報告集合
db.createCollection('data_quality_reports');

// 3. 爬蟲執行日誌集合  
db.createCollection('scraper_logs');

// 建立索引以提升查詢效能
print('🔍 Creating indexes...');

db.raw_jobs_data.createIndex({ "source": 1, "metadata.scraped_at": -1 });
db.raw_jobs_data.createIndex({ "metadata.batch_id": 1 });
db.raw_jobs_data.createIndex({ "job_data.job_id": 1, "source": 1 }, { unique: true });

print('✅ MongoDB initialization completed!');

// 插入一筆測試資料
print('📊 Inserting sample data...');

db.raw_jobs_data.insertOne({
    source: "linkedin",
    job_data: {
        job_id: "test_job_001",
        job_title: "Senior Data Engineer",
        company: "Tech Corp",
        location: "San Francisco, CA",
        salary: "$120,000 - $180,000",
        description: "Looking for a Senior Data Engineer to join our growing team..."
    },
    metadata: {
        scraped_at: new Date(),
        batch_id: "batch_" + new Date().toISOString().slice(0,10),
        scraper_version: "1.0.0",
        source_url: "https://linkedin.com/jobs/test_job_001"
    },
    data_quality: {
        completeness_score: 0.95,
        flags: []
    }
});

print('🎉 Sample data inserted successfully!');