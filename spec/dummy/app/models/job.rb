class Job < ApplicationRecord
  self.table_name = "tasks"

  include QueueIt::QueableByName
end
