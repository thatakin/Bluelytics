import matplotlib
matplotlib.use('Agg') # Use a non-interactive backend for matplotlib
import matplotlib.pyplot as plt # Import matplotlib for plotting
import matplotlib.dates as mdates # For date formatting on plots
import io # For in-memory file operations
import base64 # For encoding images to base64
import pandas as pd # Import pandas for data manipulation

from collections import Counter # For counting occurrences in posts
import re # For regex
import nltk
from nltk.corpus import stopwords # For stopword removal
import string # For punctuation
from flask import Flask, render_template, request, redirect, url_for, send_file, flash
import os
import pytz
from datetime import datetime
import uuid # For generating unique filenames

import bluesky_core # Import your Bluesky core functions

try:
    nltk.data.find('corpora/stopwords')
except nltk.downloader.DownloadError:
    nltk.download('stopwords')


app = Flask(__name__)
app.config['SECRET_KEY'] = os.urandom(24) # Needed for flash messages

# Configuration
UPLOAD_FOLDER = 'user_data'
UPLOAD_DIRECTORY_PATH = os.path.join(app.root_path, UPLOAD_FOLDER)

os.makedirs(UPLOAD_DIRECTORY_PATH, exist_ok=True)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['UPLOAD_FOLDER_PATH'] = UPLOAD_DIRECTORY_PATH

# Get a list of timezones
COMMON_TIMEZONES = sorted([tz for tz in pytz.all_timezones if '/' in tz])

# Initialize the stopwords and punctuation
STOPWORDS = set(stopwords.words('english'))
PUNCTUATION = set(string.punctuation)

# Helper function for text analysis
def get_top_topics(posts_data: list, num_topics: int = 3):
    """
    Analyses post content to find the most frequent non-stopwords
    """
    all_words = []
    for post in posts_data:
        text = post.get('Post', '')
        #Convert to lowercase
        text = text.lower()
        # Remove URLs
        text = re.sub(r'http\S+|www\S+', ' ', text)
        # Remove punctuation
        text = ''.join(char for char in text if char not in PUNCTUATION)
        # Split into words and filter out stopwords and short words
        words = [word for word in text.split() if word not in STOPWORDS and len(word) > 2]
        all_words.extend(words)
    if not all_words:
        return []
    
    word_counts = Counter(all_words)
    # Get the most common words, exlucding any potential common but uninformative words

    uninformative_words = {'bluesky', 'post', 'like', 'reply', 'repost', 'comment', 'user', 'account'}
    filtered_word_counts = {word: count for word, count in word_counts.items() if word not in uninformative_words}
    top_words = Counter(filtered_word_counts).most_common(num_topics)
    return [word for word, count in top_words]


# Helper function for best timing to post

def get_best_posting_times(posts_data: list):
    """
    Calculates the best times to post and average likes
    """
    if not posts_data:
        return None
    
    df = pd.DataFrame(posts_data)
    if 'Date' not in df.columns or 'Time' not in df.columns or df['Date'].isnull().all() or df['Time'].isnull().all():
        print("Missing 'Date' or 'Time' columns in post data")
        return None
    
    df['datetime'] = pd.to_datetime(df['Date'] + ' ' + df['Time'], format='%Y-%m-%d %H:%M:%S', errors='coerce')
    df.dropna(subset=['datetime'], inplace=True)

    if df.empty or '#likes' not in df.columns:
        return None
    
    # Extract hour
    df['hour'] = df['datetime'].dt.hour
    # Calculate average likes per hour
    avg_likes_per_hour = df.groupby('hour')['#likes'].mean()

    if avg_likes_per_hour.empty:
        return None
    
    # Find the hour with the maximum average likes:
    best_hour = avg_likes_per_hour.idxmax()

    # Format the hour
    formatted_time = datetime.strptime(f"{best_hour:02d}:00", "%H:%M").strftime("%I:%M %p")
    return formatted_time
     
# --- Main Recommendation Function ---
def generate_recommendations(posts_data: list, target_timezone_str: str, handle: str):

    recommendation_text = "Based on your post history data:"

    # 1. Get Best Posting Time
    best_time = get_best_posting_times(posts_data)
    if best_time:
        recommendation_text += f"\n- The best time to post is around {best_time} ({target_timezone_str}), "
    else:
        recommendation_text += "\n- Unable to determine the best time to post based on your data."

    # 2. Get Top Topics
    top_topics = get_top_topics(posts_data, num_topics=3)
    if top_topics:
        topics_str = ', '.join([f"'{topic}'" for topic in top_topics])
        recommendation_text += f"focusing on topics like: {topics_str}."
    else:
        recommendation_text += " and we couldn't identify specific topics."

    return recommendation_text


# --- Helper Function to Generate Plots (from your visualizer.py) ---
def generate_charts(posts_data: list, target_timezone_str: str, handle: str):
    """
    Generates Matplotlib charts from post data and returns them as base64 encoded PNGs.
    """
    charts = {}
    if not posts_data:
        print("No posts data to generate charts.")
        return charts

    try:
        # Convert list of dictionaries to pandas DataFrame
        df = pd.DataFrame(posts_data)

        # Prepare the data: Combine 'Date' and 'Time' into a single datetime column
        # Ensure 'Date' and 'Time' columns exist and are not empty
        if 'Date' not in df.columns or 'Time' not in df.columns or df['Date'].isnull().all() or df['Time'].isnull().all():
             print("Missing 'Date' or 'Time' columns in post data. Cannot generate datetime for charts.")
             flash("Missing date/time information in fetched posts. Cannot generate charts.", "warning")
             return charts

        df['datetime'] = pd.to_datetime(df['Date'] + ' ' + df['Time'], format='%Y-%m-%d %H:%M:%S', errors='coerce')
        # Drop rows where datetime conversion failed
        df.dropna(subset=['datetime'], inplace=True)
        if df.empty:
            print("DataFrame is empty after datetime conversion and dropping NaNs.")
            flash("No valid post dates found after processing. Cannot generate charts.", "warning")
            return charts

        # Sort the DataFrame by datetime (important for chronological plots)
        df = df.sort_values(by='datetime')

        # --- Chart 1: Line Chart (Likes per Post) ---
        plt.figure(figsize=(14, 7))
        plt.plot(df['datetime'], df['#likes'], marker='o', linestyle='-', color='skyblue', label='Likes per Post')
        plt.title(f'Bluesky Post Likes Over Time ({target_timezone_str})', fontsize=16)
        plt.xlabel(f'Date and Time ({target_timezone_str})', fontsize=12)
        plt.ylabel('Number of Likes', fontsize=12)
        plt.grid(True, linestyle='--', alpha=0.7)
        plt.legend(fontsize=10)
        plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d %H:%M'))
        plt.gca().xaxis.set_major_locator(plt.AutoLocator())
        plt.gcf().autofmt_xdate()
        plt.tight_layout()
        buf = io.BytesIO()
        plt.savefig(buf, format='png')
        buf.seek(0)
        charts['chart1'] = base64.b64encode(buf.getvalue()).decode('utf-8')
        plt.close() # Close the plot to free memory

        # --- Chart 2: Bar Chart (Total Likes Per Day) ---
        df['just_date'] = df['datetime'].dt.date
        daily_likes = df.groupby('just_date')['#likes'].sum().reset_index()
        daily_likes['just_date'] = pd.to_datetime(daily_likes['just_date'])

        plt.figure(figsize=(14, 7))
        plt.bar(daily_likes['just_date'], daily_likes['#likes'], color='teal', width=0.8, label='Total Daily Likes')
        plt.title(f'Total Bluesky Likes Per Day ({target_timezone_str})', fontsize=16)
        plt.xlabel(f'Date ({target_timezone_str})', fontsize=12)
        plt.ylabel('Total Number of Likes', fontsize=12)
        plt.grid(axis='y', linestyle='--', alpha=0.7)
        plt.legend(fontsize=10)
        plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
        plt.gca().xaxis.set_major_locator(mdates.DayLocator(interval=1))
        plt.gcf().autofmt_xdate()
        plt.tight_layout()
        buf = io.BytesIO()
        plt.savefig(buf, format='png')
        buf.seek(0)
        charts['chart2'] = base64.b64encode(buf.getvalue()).decode('utf-8')
        plt.close() # Close the plot to free memory

        # --- Chart 3: Horizontal Bar Chart (Top Posts by Likes) ---
        top_posts = df.sort_values(by='#likes', ascending=False).head(10).copy()
        top_posts = top_posts.iloc[::-1] # Reverse for plotting ascending

        # Truncate post text for readability on the y-axis
        top_posts['truncated_post'] = top_posts['Post'].apply(
            lambda x: (x[:70] + '...' if len(x) > 70 else x)
        )

        plt.figure(figsize=(12, 8))
        plt.barh(top_posts['truncated_post'], top_posts['#likes'], color='orange')
        plt.xlabel('Number of Likes', fontsize=12)
        plt.ylabel('Post Content', fontsize=12)
        plt.title('Top 10 Bluesky Posts by Likes', fontsize=16)
        plt.grid(axis='x', linestyle='--', alpha=0.7)
        plt.tight_layout()
        buf = io.BytesIO()
        plt.savefig(buf, format='png')
        buf.seek(0)
        charts['chart3'] = base64.b64encode(buf.getvalue()).decode('utf-8')
        plt.close() # Close the plot to free memory

    except Exception as e:
        print(f"An error occurred during chart generation: {e}")
        flash(f"Error generating charts: {e}", "error")
    
    return charts

# Route for the home page input form
@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        action = request.form.get('action') # Get the value of the 'action' button
        
        # Get common form inputs
        handle = request.form.get('handle')
        app_password = request.form.get('app_password')
        include_replies = request.form.get('include_replies') == 'on'
        include_reposts = request.form.get('include_reposts') == 'on'
        from_date_str = request.form.get('from_date')
        to_date_str = request.form.get('to_date')
        selected_timezone_str = request.form.get('timezone')

        # Process the limit
        try:
            limit = int(request.form['limit'])
            if not 1 <= limit <= 500:
                flash("Please enter a post limit between 1 and 500.", "error")
                return render_template('index.html', timezones=COMMON_TIMEZONES, request=request)
        except ValueError:
            flash("Invalid limit. Please enter a number", "error")
            return render_template('index.html', timezones=COMMON_TIMEZONES, request=request)
        
        # Process timezone
        target_timezone = None
        try:
            target_timezone = pytz.timezone(selected_timezone_str)
        except pytz.UnknownTimeZoneError:
            flash(f"Invalid timezone selected: '{selected_timezone_str}'. Please try again.", "error")
            return render_template('index.html', timezones=COMMON_TIMEZONES, request=request)
        except Exception as e:
            flash(f"An unexpected error occurred while setting timezone: {e}", "error")
            return render_template('index.html', timezones=COMMON_TIMEZONES, request=request)
        
        # Process date inputs
        start_datetime_utc = None
        end_datetime_utc = None
        display_from_date = "beginning of time" # For display in analysis results
        display_to_date = "now" # For display in analysis results

        try:
            if from_date_str:
                naive_from_date = datetime.strptime(from_date_str, '%Y-%m-%d')
                aware_from_date_user_tz = target_timezone.localize(naive_from_date)
                start_datetime_utc = aware_from_date_user_tz.astimezone(pytz.utc)
                start_datetime_utc = start_datetime_utc.replace(hour=0, minute=0, second=0, microsecond=0)
                display_from_date = from_date_str

            if to_date_str:
                naive_to_date = datetime.strptime(to_date_str, '%Y-%m-%d')
                aware_to_date_user_tz = target_timezone.localize(naive_to_date)
                end_datetime_utc = aware_to_date_user_tz.astimezone(pytz.utc)
                end_datetime_utc = end_datetime_utc.replace(hour=23, minute=59, second=59, microsecond=999999)
                display_to_date = to_date_str

            if start_datetime_utc and end_datetime_utc and start_datetime_utc > end_datetime_utc:
                flash("'From Date' cannot be after 'To Date'. Please correct your dates.", "error")
                return render_template('index.html', timezones=COMMON_TIMEZONES, request=request)

        except ValueError:
            flash("Invalid date format. Please use `%Y-%m-%d`.", "error") # Corrected format message
            return render_template('index.html', timezones=COMMON_TIMEZONES, request=request)
        except Exception as e:
            flash(f"An unexpected error occurred with date parsing: {e}", "error")
            return render_template('index.html', timezones=COMMON_TIMEZONES, request=request)

        # Log in to Bluesky
        flash("Attempting to log in to Bluesky...", "info")
        access_jwt = bluesky_core.bluesky_login(handle, app_password)
        if not access_jwt:
            flash("Login failed. Please check your handle and app password.", "error")
            return render_template('index.html', timezones=COMMON_TIMEZONES, request=request)

        flash("Login successful! Now fetching posts...", "success")
        my_posts = bluesky_core.get_my_posts(access_jwt, handle, limit, include_replies=include_replies, exclude_reposts= not include_reposts, from_date=start_datetime_utc, to_date=end_datetime_utc)
        
        if not my_posts:
            flash("No posts found for the given criteria.", "warning")
            return render_template('index.html', timezones=COMMON_TIMEZONES, request=request)
        
        flash(f"Successfully fetched {len(my_posts)} posts.", "success")
        
        # Prepare posts for CSV/analysis (localized data)
        posts_for_processing = bluesky_core.prepare_post_data_for_csv(my_posts, target_timezone)
        
        if not posts_for_processing:
            flash("No data prepared for processing. This might indicate an issue with post processing.", "warning")
            return render_template('index.html', timezones=COMMON_TIMEZONES, request=request)

        if action == "download_csv":
            # Generate a unique filename for the CSV
            unique_id = uuid.uuid4().hex[:8]
            timezone_filename_part = target_timezone.tzname(datetime.now()).replace("/", "_").replace(" ", "_")
            output_filename = f'{handle.split(".")[0]}_posts_{timezone_filename_part}_{unique_id}.csv'
            full_save_path = os.path.join(app.config['UPLOAD_FOLDER_PATH'], output_filename)
            
            flash(f"Saving posts to {output_filename}...", "info")
            bluesky_core.save_posts_to_csv(posts_for_processing, full_save_path)

            flash("CSV file generated successfully!", "success")
            return redirect(url_for('download_file', filename=output_filename))

        elif action == "analyze_posts":
            flash("Generating analysis charts...", "info")
            charts_data = generate_charts(posts_for_processing, selected_timezone_str, handle)
            recommendation_text = generate_recommendations(posts_for_processing, selected_timezone_str, handle)
            
            # --- REMOVED THE UNNECESSARY RETURN HERE ---
            # if not recommendation_text:
            #     print("Could not generate recommendations. Please check the data.", "error")
            #     # Removed the return to allow analysis_results.html to render even if recommendation is empty
            
            if not charts_data:
                flash("Could not generate charts. Please check the data.", "error")
                # This return is still here because if charts can't be generated,
                # the analysis page would be quite empty.
                return render_template('index.html', timezones=COMMON_TIMEZONES, request=request)
            
            flash("Analysis complete!", "success")
            return render_template('analysis_results.html', 
                                   chart1_data=charts_data.get('chart1'),
                                   chart2_data=charts_data.get('chart2'),
                                   chart3_data=charts_data.get('chart3'),
                                   recommendation=recommendation_text, # Pass the recommendation text
                                   handle=handle,
                                   num_posts=len(posts_for_processing),
                                   display_from_date=display_from_date,
                                   display_to_date=display_to_date,
                                   display_timezone=selected_timezone_str)
        else:
            # Should not happen if buttons have action values
            flash("Invalid action.", "error")
            return render_template('index.html', timezones=COMMON_TIMEZONES, request=request)

    # For GET requests, just display the form
    return render_template('index.html', timezones=COMMON_TIMEZONES)

# Route for file download
@app.route('/download/<filename>')
def download_file(filename):
    file_path_to_check = os.path.join(app.config['UPLOAD_FOLDER_PATH'], filename)

    if not filename.endswith('.csv'):
        flash("Invalid file request. Only CSV files can be downloaded.", "error")
        return redirect(url_for('index'))

    file_path = os.path.join(app.config['UPLOAD_FOLDER_PATH'], filename)

    if os.path.exists(file_path):
        return send_file(file_path, as_attachment=True, download_name=filename, mimetype='text/csv')
    else:
        flash("File not found.", "error")
        return redirect(url_for('index'))

if __name__ == '__main__':
    app.run(debug=True)