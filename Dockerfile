# Use the official Ruby 2.7 image for ARM architecture
FROM ruby:2.7.8-bullseye

# Install Bundler version 2.2.34
RUN gem install bundler -v 2.2.34

# Set up the work directory
WORKDIR /usr/src/app

# Copy the gemspec and all required files for it
COPY queue_it.gemspec ./

# Copy the Gemfile and Gemfile.lock into the container
COPY Gemfile* ./

# Install the required gems
RUN bundle install

# Copy the rest of your application code
COPY . .

# Command to run your test suite
CMD ["bundle", "exec", "rake", "test"]
