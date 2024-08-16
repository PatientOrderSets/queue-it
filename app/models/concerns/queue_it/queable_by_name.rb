module QueueIt::QueableByName
  extend ActiveSupport::Concern

  included do
    has_many :queues,
            as: :queable, inverse_of: :queable, dependent: :destroy, class_name: 'QueueIt::Queue'

    def find_or_create_queue!(name)
      QueueIt::Queue.find_or_create_by!(queable: self, name: name)
    end

    def push_to_queue(name, nodable, in_head = true, skip_callback = false)
      if local_queue(name).empty?
        local_queue(name).push_node_when_queue_length_is_zero(nodable, skip_callback)
      elsif local_queue(name).one_node?
        local_queue(name).push_node_when_queue_length_is_one(nodable, in_head, skip_callback)
      else
        in_head ? local_queue(name).push_in_head(nodable, skip_callback) : local_queue(name).push_in_tail(nodable, skip_callback)
      end
    end

    def get_next_in_queue_by(name, nodable_attribute, attribute_value)
      get_next_node_in_queue_by(name, nodable_attribute, attribute_value)&.nodable
    end

    def get_next_node_in_queue_by(name, nodable_attribute, attribute_value)
      return if local_queue(name).empty?

      if local_queue(name).one_node?
        return local_queue(name).get_next_by_with_queue_length_one(nodable_attribute, attribute_value)
      elsif local_queue(name).two_nodes?
        return local_queue(name).get_next_by_with_queue_length_two(nodable_attribute, attribute_value)
      end

      local_queue(name).get_next_by_in_generic_queue(nodable_attribute, attribute_value)
    end

    def get_next_in_queue(name)
      get_next_node_in_queue(name)&.nodable
    end

    def get_next_node_in_queue(name)
      return if local_queue(name).empty?

      if local_queue(name).one_node?
        local_queue(name).head_node
      elsif local_queue(name).two_nodes?
        local_queue(name).get_next_in_queue_with_length_two
      else
        local_queue(name).get_next_in_queue_generic
      end
    end

    def formatted_queue(name, nodable_attribute)
      return if local_queue(name).empty?
      return [local_queue(name).head_node.nodable.send(nodable_attribute)] if local_queue(name).one_node?

      formatted_generic_queue(name, nodable_attribute)
    end

    def peek_next_in_queue_by(name, nodable_attribute, attribute_value)
      peek_next_node_in_queue_by(name, nodable_attribute, attribute_value)&.nodable
    end

    def peek_next_node_in_queue_by(name, nodable_attribute, attribute_value)
      return if local_queue(name).empty?

      if local_queue(name).one_node?
        return local_queue(name).peek_next_by_with_queue_length_one(nodable_attribute, attribute_value)
      elsif local_queue(name).two_nodes?
        return local_queue(name).peek_next_by_with_queue_length_two(nodable_attribute, attribute_value)
      end

      local_queue(name).peek_next_by_in_generic_queue(nodable_attribute, attribute_value)
    end

    def peek_next_in_queue(name)
      local_queue(name).peek_next_in_queue_generic&.nodable
    end

    def delete_queue_nodes(name)
      local_queue(name).nodes.delete_all
      local_queue(name).count_of_nodes = 0
      local_queue(name).save
    end

    def pop_from_queue(name, skip_callback = false)
      nodable = nil
      ActiveRecord::Base.transaction do
        local_queue(name).lock!

        nodable = get_next_in_queue(name)
        local_queue(name).nodes.where(nodable: nodable).find_each do |node|
          remove_node(node)
        end
      end

      after_commit_handler(name, nodable, "remove") if nodable.present? && skip_callback
      nodable
    end

    def pop_from_queue_by(name, nodable_attribute, attribute_value, skip_callback = false)
      nodable = nil
      ActiveRecord::Base.transaction do
        local_queue(name).lock!

        nodable = get_next_in_queue_by(name, nodable_attribute, attribute_value)
        local_queue(name).nodes.where(nodable: nodable).find_each do |node|
          remove_node(node)
        end
      end

      after_commit_handler(name, nodable, "remove") if nodable.present? && skip_callback
      nodable
    end

    def remove_from_queue(name, nodable, skip_callback = false)
      return if local_queue(name).empty? || local_queue(name).nodes.where(nodable: nodable).empty?

      ActiveRecord::Base.transaction do
        local_queue(name).lock!
        local_queue(name).nodes.where(nodable: nodable).find_each do |node|
          remove_node(node)
        end
      end

      after_commit_handler(name, nodable, "remove") unless skip_callback
      nodable
    end

    def connected_nodes(name)
      counter = 0
      current_node = local_queue(name).head_node
      while !current_node.nil?
        counter += 1
        current_node = current_node.child_node
      end
      counter
    end

    def local_queue(name)
      find_or_create_queue!(name)
    end

    def queue(name)
      QueueIt::QueueApi.new(self, local_queue(name))
    end

    private

    def remove_node(node)
      node.reload
      previous_node = node.parent_node
      child_node = node.child_node
      node_kind = node.kind
      node.destroy
      new_child_node_kind = node_kind == 'head' ? node_kind : child_node&.kind
      child_node&.update!(parent_node: previous_node, kind: new_child_node_kind)
      previous_node&.update!(kind: node_kind) if node_kind == 'tail' && previous_node&.any?
    end
  end

  def formatted_generic_queue(name, nodable_attribute)
    current_node = local_queue(name).head_node
    array = []
    while !current_node.nil?
      array.push(current_node.nodable.send(nodable_attribute))
      current_node = current_node.child_node
    end
    array
  end

  def after_commit_handler(name, nodable, operation)
    QueueIt.queue_callback.call(self, name, nodable, operation) if QueueIt.queue_callback.respond_to?(:call)
  end
end
