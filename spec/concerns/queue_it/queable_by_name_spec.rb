describe 'Concerns::QueableByName' do
  let(:task) { create(:job) }

  describe '#push' do
    context 'add node to queue' do
      let(:nodable) { create(:user) }

      it 'expect queue to not be empty' do
        a = create(:user)
        b = create(:user)

        expect(task.queues.count).to eq(0)

        task.queue("my_users").push(a)
        task.queue("my_users").push(b)

        expect(task.queues.count).to eq(1)
        expect(task.queue("my_users").nodes.count).to eq(2)
        expect(task.queue("my_users").head_node.nodable).to eq(a)
        expect(task.queue("my_users").tail_node.nodable).to eq(b)
      end
    end
  end

  describe '#unshift' do
    context 'add node to queue' do
      let(:nodable) { create(:user) }

      it 'expect queue to not be empty' do
        a = create(:user)
        b = create(:user)

        expect(task.queues.count).to eq(0)

        task.queue("my_users").unshift(a)
        task.queue("my_users").unshift(b)

        expect(task.queues.count).to eq(1)
        expect(task.queue("my_users").nodes.count).to eq(2)
        expect(task.queue("my_users").head_node.nodable).to eq(b)
        expect(task.queue("my_users").tail_node.nodable).to eq(a)
      end
    end
  end

  describe '#pop' do
    it "should return the first node in the queue sorted by field" do
      task.queue("cops").push(create(:user, name: "John"))
      task.queue("cops").push(create(:user, name: "Bob"))

      result = task.queue("cops").pop(
        sort_exp: :name,
        sort_order: :desc
      )
      expect(result.name).to eq("Bob")
    end

    it "should sort the queue by the given sort expression and order and return the last value" do
      task.queue("cops").push(create(:user, name: "Eric"))
      task.queue("cops").push(create(:user, name: "Xia"))
      task.queue("cops").push(create(:user, name: "John"))

      dobs = {
        "Xia" => Date.new(1992, 1, 1),
        "Eric" => Date.new(1990, 1, 1),
        "John" => Date.new(1991, 1, 1)
      }

      result = task.queue("cops").pop(
        sort_exp: ->(u) { dobs[u.name] },
        sort_order: :desc
      )
      expect(result.name).to eq("Eric")
    end

    it "should return the last node in the queue that match the given filter expression" do
      task.queue("cops").push(create(:user, name: "Bob"))
      task.queue("cops").push(create(:user, name: "Xia"))
      task.queue("cops").push(create(:user, name: "John"))

      result = task.queue("cops").shift(
        filter_exp: ->(u) { u.name != "John" }
      )
      expect(result.name).to eq("Bob")
    end
  end

  describe '#shift' do
    it "should return the first node in the queue sorted by field" do
      task.queue("cops").push(create(:user, name: "John"))
      task.queue("cops").push(create(:user, name: "Bob"))

      result = task.queue("cops").shift(
        sort_exp: :name,
        sort_order: :desc
      )
      expect(result.name).to eq("John")
    end

    it "should sort the queue by the given sort expression and order and return the last value" do
      task.queue("cops").push(create(:user, name: "Eric"))
      task.queue("cops").push(create(:user, name: "Xia"))
      task.queue("cops").push(create(:user, name: "John"))

      dobs = {
        "Xia" => Date.new(1992, 1, 1),
        "Eric" => Date.new(1990, 1, 1),
        "John" => Date.new(1991, 1, 1)
      }

      result = task.queue("cops").shift(
        sort_exp: ->(u) { dobs[u.name] },
        sort_order: :asc
      )
      expect(result.name).to eq("Eric")
    end

    it "should return the first node in the queue that match the given filter expression" do
      task.queue("cops").push(create(:user, name: "Bob"))
      task.queue("cops").push(create(:user, name: "Xia"))
      task.queue("cops").push(create(:user, name: "John"))

      result = task.queue("cops").shift(
        filter_exp: ->(u) { u.name != "John" }
      )
      expect(result.name).to eq("Bob")
    end
  end

  describe '#nodables' do
    it "should return the nodes in the queue sorted using the given sort expression and sort order" do
      task.queue("cops").push(create(:user, name: "Bob"))
      task.queue("cops").push(create(:user, name: "Xia"))
      task.queue("cops").push(create(:user, name: "John"))

      dobs = {
        "Xia" => Date.new(1992, 1, 1),
        "Bob" => Date.new(1990, 1, 1),
        "John" => Date.new(1991, 1, 1)
      }

      result = task.queue("cops").nodables(
        sort_exp: ->(u) { dobs[u.name] },
        sort_order: :asc
      )
      expect(result.map(&:name)).to eq(%w(Bob John Xia))

      result = task.queue("cops").nodables(
        sort_exp: ->(u) { dobs[u.name] },
        sort_order: :desc
      )
      expect(result.map(&:name)).to eq(%w(Xia John Bob))
    end

    it "should return the nodes in the queue that match the given filter expression" do
      task.queue("cops").push(create(:user, name: "Bob"))
      task.queue("cops").push(create(:user, name: "Xia"))
      task.queue("cops").push(create(:user, name: "John"))

      result = task.queue("cops").nodables(
        filter_exp: ->(u) { u.name != "Xia" },
        sort_exp: :name
      )
      expect(result.map(&:name)).not_to include("Xia")
    end

    it "should return the nodes in the queue that match the given filter expression and sorted using the given sort expression and sort order" do
      task.queue("cops").push(create(:user, name: "Bob"))
      task.queue("cops").push(create(:user, name: "Xia"))
      task.queue("cops").push(create(:user, name: "John"))

      dobs = {
        "Xia" => Date.new(1992, 1, 1),
        "Bob" => Date.new(1990, 1, 1),
        "John" => Date.new(1991, 1, 1)
      }

      result = task.queue("cops").nodables(
        sort_exp: ->(u) { dobs[u.name] },
        filter_exp: ->(u) { u.name.include?("o") },
        sort_order: :desc
      )
      expect(result.map(&:name)).to eq(%w(John Bob))
    end

    it "should return the nodes in the order they were added to the queue if no sort expression is given" do
      task.queue("cops").push(create(:user, name: "Bob"))
      task.queue("cops").push(create(:user, name: "Xia"))
      task.queue("cops").unshift(create(:user, name: "John"))

      result = task.queue("cops").nodables
      expect(result.map(&:name)).to eq(%w(John Bob Xia))
    end
  end

  describe '#peek' do
    it "should return the first node in the queue sorted by field" do
      task.queue("cops").push(create(:user, name: "John"))
      task.queue("cops").push(create(:user, name: "Bob"))

      result = task.queue("cops").peek(
        sort_exp: :name,
        sort_order: :desc
      )
      expect(result.name).to eq("John")
      expect(task.queue("cops").nodes.count).to eq(2)
    end

    it "should sort the queue by the given sort expression and order and return the last value" do
      task.queue("cops").push(create(:user, name: "Eric"))
      task.queue("cops").push(create(:user, name: "Xia"))
      task.queue("cops").push(create(:user, name: "John"))

      dobs = {
        "Xia" => Date.new(1992, 1, 1),
        "Eric" => Date.new(1990, 1, 1),
        "John" => Date.new(1991, 1, 1)
      }

      result = task.queue("cops").peek(
        sort_exp: ->(u) { dobs[u.name] },
        sort_order: :asc
      )
      expect(result.name).to eq("Eric")
    end

    it "should return the first node in the queue that match the given filter expression" do
      task.queue("cops").push(create(:user, name: "Bob"))
      task.queue("cops").push(create(:user, name: "Xia"))
      task.queue("cops").push(create(:user, name: "John"))

      result = task.queue("cops").peek(
        filter_exp: ->(u) { u.name != "John" }
      )
      expect(result.name).to eq("Bob")
    end
  end

  describe '#remove' do
    it "should remove the nodable from the queue" do
      john = create(:user, name: "John")
      bob = create(:user, name: "Bob")
      task.queue("cops").push(john)
      task.queue("cops").push(bob)

      task.queue("cops").remove(john)
      expect(task.queue("cops").nodes.count).to eq(1)
    end
  end

  describe '#contains?' do
    it "should return true if nodable is in the queue" do
      john = create(:user, name: "John")
      bob = create(:user, name: "Bob")
      task.queue("cops").push(john)
      task.queue("cops").push(bob)

      expect(task.queue("cops").contains?(john)).to be_truthy
    end

    it "should return false if nodable is not in the queue" do
      john = create(:user, name: "John")
      bob = create(:user, name: "Bob")
      task.queue("cops").push(john)

      expect(task.queue("cops").contains?(bob)).to be_falsey
    end
  end

  describe '#suppress_callbacks' do
    it "should not call queue_callback when queue is modified inside the suppress_callbacks block" do
      queue = task.queue("cops")

      queue.push(create(:user, name: "Bob"))
      queue.push(create(:user, name: "Xia"))

      queue_callback = double("QueueCallback")
      expect(queue_callback).to receive(:call).never
      expect(QueueIt).to receive(:queue_callback).and_return(queue_callback).never

      nodable = queue.suppress_callbacks do
        queue.pop
      end

      expect(nodable.name).to eq("Xia")
    end

    it "should call queue_callback when queue is modified outside the suppress_callbacks block" do
      queue = task.queue("cops")

      queue.push(create(:user, name: "Bob"))
      queue.push(create(:user, name: "Xia"))

      queue_callback = double("QueueCallback")
      expect(queue_callback).to receive(:call).once
      expect(QueueIt).to receive(:queue_callback).and_return(queue_callback).twice

      nodable = queue.pop

      expect(nodable.name).to eq("Xia")
    end

    context "when no block is given" do
      it "should suppress callbacks on that instance until resume_callbacks is called" do
        queue = task.queue("cops")

        queue.push(create(:user, name: "Bob"))
        queue.push(create(:user, name: "Xia"))

        queue_callback = double("QueueCallback")
        expect(queue_callback).to receive(:call).once
        expect(QueueIt).to receive(:queue_callback).and_return(queue_callback).twice

        queue.suppress_callbacks
        nodable = queue.pop
        expect(nodable.name).to eq("Xia")

        queue.resume_callbacks
        nodable = queue.pop
        expect(nodable.name).to eq("Bob")
      end
    end
  end

  it "should call after_queue_changed when queue is modified" do
    task = JobWithListener.create
    queue = task.queue("cops")

    bob = create(:user, name: "Bob")
    xia = create(:user, name: "Xia")
    john = create(:user, name: "John")

    queue.push(bob)
    queue.push(xia)

    expect(task).to receive(:after_queue_changed).with(queue.queue.name, xia, "pop").once
    expect(task).to receive(:after_queue_changed).with(queue.queue.name, john, "push").once

    queue.pop
    queue.push(john)
  end

  it "filter expression should execute in the context of the declaring method" do
    task = JobWithAttributes.create
    task.age = 30

    bob = create(:user, name: "Bob")
    xia = create(:user, name: "Xia")
    john = create(:user, name: "John")

    task.queue('cops').push(bob)
    task.queue('cops').push(xia)
    task.queue('cops').push(john)

    expect(task.sorted_cops.map(&:name)).to eq(%w(Bob Xia John))
  end
end

class JobWithListener < Job
  def after_queue_changed(queue, nodable, event); end
end

class JobWithAttributes < Job
  attr_accessor :age

  def sorted_cops
    queue('cops').nodables(
      sort_exp: ->(u) { u.name.length + age },
      sort_order: :asc
    )
  end
end
