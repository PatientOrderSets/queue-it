require "queue_it/engine"

module QueueIt
  extend self

  attr_accessor :queue_callback

  # You can add, in this module, your own configuration options as in the example below...
  #
  # attr_writer :my_option
  #
  # def my_option
  #   return "Default Value" unless @my_option
  #   @my_option
  # end
  #
  # Then, you can customize the default behaviour (typically in a Rails initializer) like this:
  #
  # QueueIt.setup do |config|
  #   config.root_url = "Another value"
  # end

  def setup
    yield self
    require "queue_it"
  end
end
