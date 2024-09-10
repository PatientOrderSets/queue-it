module QueueIt::QueableByName
  extend ActiveSupport::Concern

  included do
    has_many :queues,
            as: :queable, inverse_of: :queable, dependent: :destroy, class_name: 'QueueIt::Queue'

    def find_or_create_queue!(name)
      QueueIt::Queue.find_or_create_by!(queable: self, name: name)
    end

    def delete_queue_nodes(name)
      queue(name).nodes.delete_all
      queue(name).count_of_nodes = 0
      queue(name).save
    end

    def connected_nodes(name)
      counter = 0
      current_node = queue(name).head_node
      while !current_node.nil?
        counter += 1
        current_node = current_node.child_node
      end
      counter
    end

    def queue(name)
      QueueIt::QueueApi.new(self, find_or_create_queue!(name))
    end
  end
end
