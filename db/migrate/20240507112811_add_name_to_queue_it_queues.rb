class AddNameToQueueItQueues < ActiveRecord::Migration[6.1]
  def change
    add_column :queue_it_queues, :name, :string
    add_index :queue_it_queues, [:name, :queable_type, :queable_id], unique: true
  end
end
