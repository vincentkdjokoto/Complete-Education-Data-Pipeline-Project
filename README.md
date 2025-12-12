# Complete-Education-Data-Pipeline-Project

# OECD Education Data Pipeline

## 📊 Project Overview
An automated data pipeline that extracts OECD education statistics, cleans and transforms the data, loads it into a SQLite database, and provides an interactive dashboard for analysis.

## 🎯 Key Question Answered
**"How can we automate the collection and storage of dynamic public education data?"**

This project demonstrates:
1. **Automated Data Collection**: Scheduled extraction from OECD API
2. **Robust Processing**: Data cleaning, transformation, and validation
3. **Structured Storage**: SQLite database with proper schema design
4. **Containerization**: Docker for reproducible deployment
5. **Visualization**: Interactive dashboard for data exploration

## 🏗️ Architecture


## 🚀 Quick Start

### **Option 1: Docker (Recommended)**
```bash
# Clone the repository
git clone https://github.com/yourusername/education-data-pipeline.git
cd education-data-pipeline

# Build and run with Docker Compose
docker-compose up --build

# Access the dashboard at http://localhost:8501
