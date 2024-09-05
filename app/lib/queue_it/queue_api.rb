class QueueIt::QueueApi
  SortableItem = Struct.new(:value, :index)

  attr_reader :queue, :emit_callbacks, :queable

  def initialize(queable, queue)
    @queable = queable
    @queue = queue
    @emit_callbacks = true
  end

  def suppress_callbacks
    if block_given?
      begin
        @emit_callbacks = false
        yield
      ensure
        @emit_callbacks = true
      end
    else
      @emit_callbacks = false
    end
  end

  def resume_callbacks
    @emit_callbacks = true
  end

  def nodables(filter_exp: nil, sort_exp: nil, sort_order: :asc)
    ActiveRecord::Base.transaction do
      nodables = queue.nodables
      if filter_exp.present?
        filter_exp = create_expression(filter_exp)
        nodables = execute_filter_exp(filter_exp, nodables)
      end

      if sort_exp.present?
        sort_exp = create_sort_exp(create_expression(sort_exp), sort_order)
        nodables = execute_sort_exp(sort_exp, nodables)
      end

      nodables
    end
  end

  def contains?(nodable)
    queue.contains?(nodable)
  end

  def peek(filter_exp: nil, sort_exp: nil, sort_order: :asc)
    items = nodables(
      sort_exp: sort_exp,
      filter_exp: filter_exp,
      sort_order: sort_order
    )

    items.first
  end

  def push(nodable)
    if queue.empty?
      queue.push_node_when_queue_length_is_zero(nodable, false)
    elsif queue.one_node?
      queue.push_node_when_queue_length_is_one(nodable, false, false)
    else
      queue.push_in_tail(nodable, false)
    end

    emit_event(nodable, "push")

    self
  end

  def unshift(nodable)
    if queue.empty?
      queue.push_node_when_queue_length_is_zero(nodable, false)
    elsif queue.one_node?
      queue.push_node_when_queue_length_is_one(nodable, true, false)
    else
      queue.push_in_head(nodable, false)
    end

    emit_event(nodable, "unshift")

    self
  end

  def remove(nodable)
    ActiveRecord::Base.transaction do
      queue.lock!

      nodes = queue.nodes.where(nodable: nodable)
      nodes.find_each { |node| remove_node(node) }
      nodes.any?
    end

    emit_event(nodable, "remove")

    self
  end

  def pop(filter_exp: nil, sort_exp: nil, sort_order: :asc)
    item = delete_nodable(
      sort_exp: sort_exp,
      filter_exp: filter_exp,
      sort_order: sort_order,
      &:last
    )

    emit_event(item, "pop")

    item
  end

  def shift(filter_exp: nil, sort_exp: nil, sort_order: :asc)
    item = delete_nodable(
      sort_exp: sort_exp,
      filter_exp: filter_exp,
      sort_order: sort_order,
      &:first
    )

    emit_event(item, "shift")

    item
  end

  def method_missing(method_name, *args, &block)
    queue.send(method_name, *args, &block)
  end

  def respond_to_missing?(method_name, include_private = false)
    queue.respond_to?(method_name, include_private) || super
  end

  private

  def delete_nodable(filter_exp: nil, sort_exp: nil, sort_order: :asc)
    ActiveRecord::Base.transaction do
      queue.lock!

      items = nodables(
        sort_exp: sort_exp,
        filter_exp: filter_exp,
        sort_order: sort_order
      )

      nodable = yield items

      queue.nodes.where(nodable: nodable).find_each do |node|
        remove_node(node)
      end

      nodable
    end
  end

  def create_expression(exp)
    return exp if exp.is_a?(Proc)

    Proc.new do |node|
      node.send(exp)
    end
  end

  def create_sort_exp(exp, order)
    return if exp.nil?

    case order
    when :desc
      ->(a, b) do
        [exp.call(b.value), b.index] <=> [exp.call(a.value), a.index]
      end
    when :asc
      ->(a, b) do
        [exp.call(a.value), a.index] <=> [exp.call(b.value), b.index]
      end
    else
      raise ArgumentError.new("Invalid sort order #{order}")
    end
  end

  def execute_filter_exp(filter_exp, _nodes)
    nodables.filter(&filter_exp)
  end

  def execute_sort_exp(sort_exp, nodes)
    nodes.map.with_index { |n, i| SortableItem.new(n, i) }.sort(&sort_exp).map(&:value)
  end

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

  def emit_event(nodable, event)
    return unless emit_callbacks

    if QueueIt.queue_callback.respond_to?(:call)
      QueueIt.queue_callback.call(queable, queue.name, nodable, event)
    end

    if queable.respond_to?(:after_queue_changed)
      queable.after_queue_changed(queue.name, nodable, event)
    end
  rescue StandardError => e
    message = "Error while emitting event \"#{event}\" for queue \"#{queue.name}\""
    message << " \n#{e.message}\n#{e.backtrace.join("\n")}"
    Rails.logger.warn(message)
  end
end
