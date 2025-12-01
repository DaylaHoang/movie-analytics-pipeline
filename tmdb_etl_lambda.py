import boto3
import json
import os
import time
import urllib.request
import urllib.error
import urllib.parse
from concurrent.futures import ThreadPoolExecutor
import logging
from io import StringIO
import csv
from datetime import datetime
import statistics

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# AWS S3 Configuration
S3_BUCKET_NAME = os.environ.get('S3_BUCKET_NAME', "2025tmdbmoviedata")
S3_FILE_NAME = os.environ.get('S3_FILE_NAME', "movies_data_enriched.csv")

# TMDB API Configuration
API_KEY = os.environ.get('TMDB_API_KEY', "728c7b4f5730549db84b7cafe2e0d30c")
BASE_URL = "<https://api.themoviedb.org/3>"

# Lambda configuration
MAX_PAGES = int(os.environ.get('MAX_PAGES', 5))
MAX_DETAILS = int(os.environ.get('MAX_DETAILS', 50))
MAX_WORKERS = int(os.environ.get('MAX_WORKERS', 5))  # For parallel processing

def make_api_request(url, retries=3, backoff_factor=0.5):
    """Makes an API request with retry logic"""
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(url) as response:
                return json.loads(response.read().decode('utf-8'))
        except urllib.error.HTTPError as e:
            if e.code in [429, 500, 502, 503, 504]:
                sleep_time = backoff_factor * (2 ** attempt)
                logger.warning(f"Request failed with status {e.code}. Retrying in {sleep_time} seconds...")
                time.sleep(sleep_time)
                continue
            else:
                logger.error(f"HTTP Error: {e.code} - {e.reason}")
                raise
        except urllib.error.URLError as e:
            logger.error(f"URL Error: {e.reason}")
            raise
        except Exception as e:
            logger.error(f"Request Error: {str(e)}")
            raise
    
    # If we get here, all retries have failed
    raise Exception(f"Failed to fetch data from {url} after {retries} attempts")

def fetch_movies(max_pages=5):
    """Extracts movie data from TMDB API"""
    movies_list = []
    movie_ids_seen = set()  # Track movie IDs to prevent duplicates from API

    for page in range(1, max_pages + 1):
        url = f"{BASE_URL}/movie/popular?api_key={API_KEY}&language=en-US&page={page}"

        try:
            data = make_api_request(url)
            
            for movie in data.get("results", []):
                movie_id = movie["id"]
                
                # Skip if we've already seen this movie ID
                if movie_id in movie_ids_seen:
                    logger.info(f"Skipping duplicate movie ID {movie_id} from API response")
                    continue
                    
                movie_ids_seen.add(movie_id)
                
                movie_element = {
                    "movie_id": movie_id,
                    "title": movie["title"],
                    "release_date": movie.get("release_date", ""),
                    "vote_average": movie.get("vote_average", None),
                    "vote_count": movie.get("vote_count", None),
                    "popularity": movie.get("popularity", None),
                    "overview": movie.get("overview", "No overview available"),
                    "poster_url": f"<https://image.tmdb.org/t/p/w500{movie['poster_path']}>" if movie.get("poster_path") else None
                }
                movies_list.append(movie_element)

            logger.info(f"Page {page} fetched successfully! Total unique movies: {len(movies_list)}")
        except Exception as e:
            logger.error(f"Error fetching page {page}: {str(e)}")
            if page == 1:  # If we can't even get the first page, abort
                raise
            break

        time.sleep(0.5)  # API rate limit handling

    logger.info(f"Successfully fetched {len(movies_list)} unique movies.")
    return movies_list

def fetch_movie_details(movie_id):
    """Fetches enriched movie details from TMDB API for a specific movie"""
    url = f"{BASE_URL}/movie/{movie_id}?api_key={API_KEY}&language=en-US&append_to_response=keywords"

    try:
        movie = make_api_request(url)
        
        return {
            'movie_id': movie_id,  # Include movie_id for joining later
            'budget': movie.get('budget', None),
            'revenue': movie.get('revenue', None),
            'runtime': movie.get('runtime', None),
            'status': movie.get('status', ''),
            'tagline': movie.get('tagline', ''),
            'genres': ', '.join([g['name'] for g in movie.get('genres', [])]),
            'production_companies': ', '.join([c['name'] for c in movie.get('production_companies', [])]),
            'spoken_languages': ', '.join([l['name'] for l in movie.get('spoken_languages', [])]),
            'original_language': movie.get('original_language', ''),
            'adult': str(movie.get('adult', False)),
            'homepage': movie.get('homepage', ''),
            'imdb_id': movie.get('imdb_id', ''),
            'keywords': ', '.join([k['name'] for k in movie.get('keywords', {}).get('keywords', [])])
        }
    except Exception as e:
        logger.error(f"Failed to fetch details for movie {movie_id}: {str(e)}")
        return {'movie_id': movie_id}  # Return at least the ID so we can join later

def enrich_movie_data_parallel(movies_list, max_details=None, max_workers=5):
    """Enriches movie data using parallel processing"""
    # Ensure we have no duplicates in the input list
    unique_movie_ids = {}
    unique_movies = []
    
    for movie in movies_list:
        movie_id = movie["movie_id"]
        if movie_id not in unique_movie_ids:
            unique_movie_ids[movie_id] = True
            unique_movies.append(movie)
    
    if len(unique_movies) < len(movies_list):
        logger.info(f"Removed {len(movies_list) - len(unique_movies)} duplicate movies before enrichment")
    
    movies_to_process = unique_movies[:max_details] if max_details else unique_movies
    movie_ids = [movie["movie_id"] for movie in movies_to_process]

    # Use ThreadPoolExecutor for parallel API calls
    details_list = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        details_list = list(executor.map(fetch_movie_details, movie_ids))

    # Create a dict for quick lookup of details by movie_id
    details_dict = {detail['movie_id']: detail for detail in details_list if detail}

    # Join the details with the original movies_list
    enriched_movies = []
    for movie in movies_to_process:
        movie_id = movie["movie_id"]
        if movie_id in details_dict:
            # Remove movie_id from details to avoid duplication
            details = details_dict[movie_id].copy()
            details.pop('movie_id', None)
            # Merge the dictionaries
            enriched_movie = {**movie, **details}
            enriched_movies.append(enriched_movie)
        else:
            enriched_movies.append(movie)

    return enriched_movies

def calculate_statistics(movies_list):
    """Calculate mean and median for numerical features"""
    # Initialize dictionaries to store values for statistical calculations
    numerical_features = ['vote_average', 'vote_count', 'popularity', 'runtime', 'budget', 'revenue']
    values = {feature: [] for feature in numerical_features}
    
    # Collect all non-None values
    for movie in movies_list:
        for feature in numerical_features:
            if feature in movie and movie[feature] is not None:
                values[feature].append(float(movie[feature]))  # Ensure values are floats
    
    # Calculate statistics
    stats = {}
    for feature in numerical_features:
        if values[feature]:
            # Use median for features likely to have outliers (budget, revenue)
            if feature in ['budget', 'revenue', 'vote_count']:
                stats[feature] = statistics.median(values[feature])
                logger.info(f"Median {feature}: {stats[feature]}")
            # Use mean for more normally distributed features
            else:
                stats[feature] = statistics.mean(values[feature])
                logger.info(f"Mean {feature}: {stats[feature]}")
        else:
            # Fallback to 0 if no data available
            stats[feature] = 0
            logger.warning(f"No data available for {feature}, using 0")
            
    return stats

def clean_transform_data(movies_list):
    """Performs data cleaning, normalization, and feature engineering using mean/median imputation"""
    if not movies_list:
        logger.warning("No movie data to process!")
        return []
    
    # Calculate statistics for imputation
    stats = calculate_statistics(movies_list)
    
    cleaned_movies = []
    
    for movie in movies_list:
        # Impute missing values with mean or median
        movie['vote_average'] = movie.get('vote_average') if movie.get('vote_average') is not None else stats['vote_average']
        movie['vote_count'] = movie.get('vote_count') if movie.get('vote_count') is not None else stats['vote_count']
        movie['popularity'] = movie.get('popularity') if movie.get('popularity') is not None else stats['popularity']
        movie['runtime'] = movie.get('runtime') if movie.get('runtime') is not None else stats['runtime']
        movie['budget'] = movie.get('budget') if movie.get('budget') is not None else stats['budget']
        movie['revenue'] = movie.get('revenue') if movie.get('revenue') is not None else stats['revenue']
        
        # Fill non-numerical missing values
        movie.setdefault('overview', 'No overview available')
        movie.setdefault('tagline', '')
        movie.setdefault('genres', 'Unknown')
        movie.setdefault('production_companies', 'Unknown')
        movie.setdefault('spoken_languages', 'Unknown')
        movie.setdefault('original_language', 'Unknown')
        movie.setdefault('keywords', '')
        
        # Convert release_date to a standardized format
        if 'release_date' in movie and movie['release_date']:
            try:
                # Parse the date
                date_obj = datetime.strptime(movie['release_date'], '%Y-%m-%d')
                movie['release_year'] = date_obj.year
            except (ValueError, TypeError):
                movie['release_year'] = None
        else:
            movie['release_year'] = None
        
        # Feature Engineering
        movie['profit'] = movie['revenue'] - movie['budget']
        
        # Avoid division by zero for ROI calculation
        if movie['budget'] > 0:
            movie['roi'] = (movie['revenue'] - movie['budget']) / movie['budget']
        else:
            # If budget is 0 or close to 0, set a default value or mark as undefined
            movie['roi'] = 0 if movie['revenue'] == 0 else float('inf')
        
        # Categorize popularity
        if movie['popularity'] < 100:
            movie['popularity_category'] = 'Low'
        elif movie['popularity'] < 500:
            movie['popularity_category'] = 'Medium'
        else:
            movie['popularity_category'] = 'High'
        
        cleaned_movies.append(movie)
    
    # Deduplicate records - this is now just a safety check since we're deduplicating earlier
    unique_movies = {}
    for movie in cleaned_movies:
        movie_id = movie['movie_id']
        if movie_id not in unique_movies:
            unique_movies[movie_id] = movie
    
    if len(unique_movies) < len(cleaned_movies):
        logger.info(f"Removed {len(cleaned_movies) - len(unique_movies)} duplicate movies during cleaning")
    
    logger.info("Data cleaning and feature engineering completed.")
    return list(unique_movies.values())

def upload_to_s3(movies_data):
    try:
        csv_buffer = StringIO()
        if not movies_data:
            writer = csv.DictWriter(csv_buffer, fieldnames=["movie_id", "title", "message"])
            writer.writeheader()
            writer.writerow({"movie_id": 0, "title": "No Data", "message": "No movie data was processed"})
        else:
            all_fields = sorted(set().union(*(m.keys() for m in movies_data)))
            writer = csv.DictWriter(csv_buffer, fieldnames=all_fields)
            writer.writeheader()
            writer.writerows(movies_data)

        # ✅ Use current date to create a new file name each day
        date_suffix = datetime.now().strftime("%Y-%m-%d")
        file_key = f"daily_outputs/movies_data_{date_suffix}.csv"

        s3 = boto3.client("s3")
        s3.put_object(Bucket=S3_BUCKET_NAME, Key=file_key, Body=csv_buffer.getvalue())
        logger.info(f"Uploaded daily ETL to s3://{S3_BUCKET_NAME}/{file_key}")
        return True
    except Exception as e:
        logger.error(f"Upload failed: {e}")
        raise

def lambda_handler(event, context):
    logger.info("Starting ETL Process...")

    try:
        # Step 1: Extract Basic Movie Data
        movies_list = fetch_movies(max_pages=MAX_PAGES)

        if not movies_list:
            raise Exception("Failed to fetch movies")

        # Step 2: Enrich with Detailed Information (using parallel processing)
        enriched_movies = enrich_movie_data_parallel(
            movies_list,
            max_details=MAX_DETAILS,
            max_workers=MAX_WORKERS
        )

        # Step 3: Clean, Transform, and Engineer Features
        cleaned_movies = clean_transform_data(enriched_movies)

        # Step 4: Load Data to S3
        upload_to_s3(cleaned_movies)

        logger.info("ETL Pipeline Execution Completed Successfully.")
        return {
            "statusCode": 200,
            "body": json.dumps({
                "status": "Success",
                "movies_processed": len(cleaned_movies),
                "destination": f"s3://{S3_BUCKET_NAME}/{S3_FILE_NAME}"
            })
        }

    except Exception as e:
        logger.error(f"ETL Pipeline Failed: {str(e)}")
        return {
            "statusCode": 500,
            "body": json.dumps({
                "status": "Failure",
                "error": str(e)
            })
        }