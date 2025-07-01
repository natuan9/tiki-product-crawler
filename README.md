# tiki-product-crawler

-- =========================================
-- PostgreSQL Database and Table Setup
-- =========================================

-- 1. CREATE DATABASE
CREATE DATABASE tiki_products;

-- =========================================
-- 2. CONNECT TO THE DATABASE AND CREATE TABLE
-- =========================================

-- Connect to your database: \c tiki_products

-- Create products table
CREATE TABLE products (
    id BIGINT PRIMARY KEY,
    name TEXT,
    url_key TEXT,
    price DECIMAL(15,2),
    description TEXT,
    images JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- =========================================
-- 3. VERIFY SETUP
-- =========================================

-- Check table structure
\d products
