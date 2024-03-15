# Streamlit Golf

## Overview

This Streamlit application is designed for managing and visualizing data for a Golf League. It integrates with Snowflake to fetch and display data, including leaderboards, player selections, and tournament trends. The app provides a user-friendly interface for golf enthusiasts to track their  league's progress, analyze player performance, and make informed decisions for their fantasy picks.

## Features

- **Dynamic Leaderboard**: Displays the current standings of league members based on the ongoing tournament results.
- **Tournament Trends**: Visualizes the score trendline for each team across a tournament.
- **Trophy Room**: Showcases past winners and their winning scores, celebrating the history and achievements within the league.
- **Analysis Tools**: Offers various tools for deeper analysis of player performances and tournament statistics.
- **Make Your Picks**: Allows users to select their team for upcoming tournaments.
- **Admin Tools**: Provides administrative functionalities to manage the league, tournaments, and user access.
- **App Status**: Displays the current status of the app, including the last data refresh time and any maintenance notices.

## Getting Started

### Prerequisites

- Python 3.6 or later
- Streamlit
- Snowflake account and credentials

### Installation

1. Clone the repository to your local machine.
2. Install the required Python packages:

```bash
pip install -r requirements.txt
```

3. Set up your Snowflake credentials. This can be done by creating a `.env` file in the root directory and adding your Snowflake user, password, account, and other necessary details.

### Running the App

To run the app, navigate to the project directory in your terminal and execute:

```bash
streamlit run Home_Leaderboard.py
```

This will start the Streamlit server and open the app in your default web browser.

## Usage

Upon launching the app, you will be presented with the main dashboard displaying the current tournament's leaderboard. Use the sidebar to navigate through different pages of the app, each offering specific functionalities and data visualizations.

## Contributing

Contributions to the Golf Fantasy League Streamlit app are welcome. Please feel free to fork the repository, make changes, and submit pull requests. For major changes, please open an issue first to discuss what you would like to change.

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- The Streamlit team for creating an amazing tool for building data apps.
- The Snowflake team for providing a powerful cloud data platform.
- All contributors and users of the Golf Fantasy League Streamlit app.
