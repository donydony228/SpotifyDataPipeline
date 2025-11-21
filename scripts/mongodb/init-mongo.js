// MongoDB Initialization Script for US Job Data Engineering Project

print('Initializing US Job Data Engineering MongoDB...');

// Switch to our project database
db = db.getSiblingDB('job_market_data');

// Create collections and insert test data
print('Creating collections...');

// 1. Raw jobs data collection
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

// 2. Data Quality Reports Collection
db.createCollection('data_quality_reports');

// 3. Scraper Logs Collection
db.createCollection('scraper_logs');

// Create indexes to improve query performance
print('🔍 Creating indexes...');

db.raw_jobs_data.createIndex({ "source": 1, "metadata.scraped_at": -1 });
db.raw_jobs_data.createIndex({ "metadata.batch_id": 1 });
db.raw_jobs_data.createIndex({ "job_data.job_id": 1, "source": 1 }, { unique: true });

print('MongoDB initialization completed!');

// Insert a sample document
print('Inserting sample data...');

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

print('Sample data inserted successfully!');