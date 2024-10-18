describe 'Concerns::QueableByName' do
  let(:task) { create(:job) }

  describe '#push' do
    context 'add node to queue' do
      let(:nodable) { create(:user) }

      it 'expect queue to not be empty' do
        user_a = create(:user)
        user_b = create(:user)
        user_c = create(:user)

        expect(task.queues.count).to eq(0)

        task.queue("my_users").push(user_a)

        expect(task.queues.count).to eq(1)
        expect(task.queue("my_users").nodes.count).to eq(1)
        expect(task.queue("my_users").count_of_nodes).to eq(1)
        expect(task.queue("my_users").nodes.where(kind: "head").first.nodable).to eq(user_a)

        task.queue("my_users").push(user_b)
        expect(task.queue("my_users").nodes.count).to eq(2)
        expect(task.queue("my_users").count_of_nodes).to eq(2)
        expect(task.queue("my_users").nodes.where(kind: "head").first.nodable).to eq(user_a)
        expect(task.queue("my_users").nodes.where(kind: "tail").first.nodable).to eq(user_b)

        task.queue("my_users").push(user_c)
        expect(task.queue("my_users").nodes.count).to eq(3)
        expect(task.queue("my_users").count_of_nodes).to eq(3)
        expect(task.queue("my_users").nodes.where(kind: "head").first.nodable).to eq(user_a)
        expect(task.queue("my_users").nodes.where(kind: "any").first.nodable).to eq(user_b)
        expect(task.queue("my_users").nodes.where(kind: "tail").first.nodable).to eq(user_c)
      end
    end
  end

  describe '#unshift' do
    context 'add node to queue' do
      let(:nodable) { create(:user) }

      it 'expect queue to not be empty' do
        user_a = create(:user)
        user_b = create(:user)
        user_c = create(:user)

        expect(task.queues.count).to eq(0)

        task.queue("my_users").unshift(user_a)
        expect(task.queue("my_users").nodes.count).to eq(1)
        expect(task.queue("my_users").count_of_nodes).to eq(1)
        expect(task.queue("my_users").nodes.where(kind: "head").first.nodable).to eq(user_a)

        task.queue("my_users").unshift(user_b)
        expect(task.queue("my_users").nodes.count).to eq(2)
        expect(task.queue("my_users").count_of_nodes).to eq(2)
        expect(task.queue("my_users").nodes.where(kind: "head").first.nodable).to eq(user_b)
        expect(task.queue("my_users").nodes.where(kind: "tail").first.nodable).to eq(user_a)

        task.queue("my_users").unshift(user_c)
        expect(task.queue("my_users").nodes.count).to eq(3)
        expect(task.queue("my_users").count_of_nodes).to eq(3)
        expect(task.queue("my_users").nodes.where(kind: "head").first.nodable).to eq(user_c)
        expect(task.queue("my_users").nodes.where(kind: "any").first.nodable).to eq(user_b)
        expect(task.queue("my_users").nodes.where(kind: "tail").first.nodable).to eq(user_a)
      end
    end
  end

  describe '#pop' do
    it "should return the first node in the queue sorted by field and nodable order" do
      edward = create(:user, name: "Edward")
      amy = create(:user, name: "Amy")
      bob = create(:user, name: "Bob")
      david = create(:user, name: "David")
      cindy = create(:user, name: "Cindy")
      frank = create(:user, name: "Frank")

      task.queue("cops")
          .push(edward)
          .push(amy)
          .push(bob)
          .push(david)
          .push(cindy)
          .push(frank)

      handle_time = {
        "Amy" => 0,
        "Bob" => 0,
        "David" => 1,
        "Cindy" => 0,
        "Edward" => 2,
        "Frank" => 0
      }

      expect(task.queue("cops").nodes.count).to eq(6)
      expect(task.queue("cops").count_of_nodes).to eq(6)
      expect(task.queue("cops").nodes.where(kind: "head").first.nodable).to eq(edward)
      any = task.queue("cops").nodes.where(kind: "any").map(&:nodable)
      expect(any).to include(amy)
      expect(any).to include(bob)
      expect(any).to include(david)
      expect(any).to include(cindy)
      expect(task.queue("cops").nodes.where(kind: "tail").first.nodable).to eq(frank)

      result = task.queue("cops").pop(
        sort_exp: ->(u) { handle_time[u.name] },
        sort_order: :asc
      )
      expect(result.name).to eq("Edward")
      expect(task.queue("cops").nodes.count).to eq(5)
      expect(task.queue("cops").count_of_nodes).to eq(5)
      expect(task.queue("cops").nodes.where(kind: "head").first.nodable).to eq(amy)
      any = task.queue("cops").nodes.where(kind: "any").map(&:nodable)
      expect(any).to include(cindy)
      expect(any).to include(david)
      expect(any).to include(bob)
      expect(task.queue("cops").nodes.where(kind: "tail").first.nodable).to eq(frank)

      result = task.queue("cops").pop(
        sort_exp: ->(u) { handle_time[u.name] },
        sort_order: :desc
      )
      expect(result.name).to eq("Amy")
      expect(task.queue("cops").nodes.count).to eq(4)
      expect(task.queue("cops").count_of_nodes).to eq(4)
      expect(task.queue("cops").nodes.where(kind: "head").first.nodable).to eq(bob)
      any = task.queue("cops").nodes.where(kind: "any").map(&:nodable)
      expect(any).to include(cindy)
      expect(any).to include(david)
      expect(task.queue("cops").nodes.where(kind: "tail").first.nodable).to eq(frank)
    end

    it "should return the first node in the queue sorted by field" do
      john = create(:user, name: "John")
      bob = create(:user, name: "Bob")
      task.queue("cops")
          .push(john)
          .push(bob)

      expect(task.queue("cops").nodes.count).to eq(2)
      expect(task.queue("cops").count_of_nodes).to eq(2)
      expect(task.queue("cops").nodes.where(kind: "head").first.nodable).to eq(john)
      expect(task.queue("cops").nodes.where(kind: "tail").first.nodable).to eq(bob)

      result = task.queue("cops").pop(
        sort_exp: :name,
        sort_order: :desc
      )
      expect(result.name).to eq("Bob")
      expect(task.queue("cops").nodes.count).to eq(1)
      expect(task.queue("cops").count_of_nodes).to eq(1)
      expect(task.queue("cops").nodes.where(kind: "head").first.nodable).to eq(john)
    end

    it "should sort the queue by the given sort expression and order and return the last value" do
      eric = create(:user, name: "Eric")
      xia = create(:user, name: "Xia")
      john = create(:user, name: "John")

      task.queue("cops")
          .push(eric)
          .push(xia)
          .push(john)

      expect(task.queue("cops").nodes.count).to eq(3)
      expect(task.queue("cops").count_of_nodes).to eq(3)
      expect(task.queue("cops").nodes.where(kind: "head").first.nodable).to eq(eric)
      expect(task.queue("cops").nodes.where(kind: "any").first.nodable).to eq(xia)
      expect(task.queue("cops").nodes.where(kind: "tail").first.nodable).to eq(john)

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
      expect(task.queue("cops").nodes.count).to eq(2)
      expect(task.queue("cops").count_of_nodes).to eq(2)
      expect(task.queue("cops").nodes.where(kind: "head").first.nodable).to eq(xia)
      expect(task.queue("cops").nodes.where(kind: "tail").first.nodable).to eq(john)
    end

    it "should return the last node in the queue that match the given filter expression" do
      bob = create(:user, name: "Bob")
      xia = create(:user, name: "Xia")
      john = create(:user, name: "John")
      task.queue("cops")
          .push(bob)
          .push(xia)
          .push(john)

      expect(task.queue("cops").nodes.count).to eq(3)
      expect(task.queue("cops").count_of_nodes).to eq(3)
      expect(task.queue("cops").nodes.where(kind: "head").first.nodable).to eq(bob)
      expect(task.queue("cops").nodes.where(kind: "any").first.nodable).to eq(xia)
      expect(task.queue("cops").nodes.where(kind: "tail").first.nodable).to eq(john)

      result = task.queue("cops").shift(
        filter_exp: ->(u) { u.name != "John" }
      )
      expect(result.name).to eq("Bob")
      expect(task.queue("cops").nodes.count).to eq(2)
      expect(task.queue("cops").count_of_nodes).to eq(2)
      expect(task.queue("cops").nodes.where(kind: "head").first.nodable).to eq(xia)
      expect(task.queue("cops").nodes.where(kind: "tail").first.nodable).to eq(john)
    end

    it "should emit callback" do
      task.queue("cops").push(create(:user, name: "John"))
      task.queue("cops").push(create(:user, name: "Bob"))

      queue_callback = double("QueueCallback")
      expect(queue_callback).to receive(:call).once
      expect(QueueIt).to receive(:queue_callback).and_return(queue_callback).twice
      result = task.queue("cops").pop(
        sort_exp: :name,
        sort_order: :desc
      )
      expect(result.name).to eq("Bob")
    end
  end

  describe '#shift' do
    it "should return the first node in the queue sorted by field and nodable order" do
      edward = create(:user, name: "Edward")
      amy = create(:user, name: "Amy")
      bob = create(:user, name: "Bob")
      david = create(:user, name: "David")
      cindy = create(:user, name: "Cindy")
      frank = create(:user, name: "Frank")

      task.queue("cops")
          .push(edward)
          .push(amy)
          .push(bob)
          .push(david)
          .push(cindy)
          .push(frank)

      handle_time = {
        "Amy" => 0,
        "Bob" => 0,
        "David" => 1,
        "Cindy" => 0,
        "Edward" => 2,
        "Frank" => 0
      }

      expect(task.queue("cops").nodes.count).to eq(6)
      expect(task.queue("cops").count_of_nodes).to eq(6)
      expect(task.queue("cops").nodes.where(kind: "head").first.nodable).to eq(edward)
      any = task.queue("cops").nodes.where(kind: "any").map(&:nodable)
      expect(any).to include(amy)
      expect(any).to include(bob)
      expect(any).to include(david)
      expect(any).to include(cindy)
      expect(task.queue("cops").nodes.where(kind: "tail").first.nodable).to eq(frank)

      result = task.queue("cops").shift(
        sort_exp: ->(u) { handle_time[u.name] },
        sort_order: :asc
      )
      expect(result.name).to eq("Amy")
      expect(task.queue("cops").nodes.count).to eq(5)
      expect(task.queue("cops").count_of_nodes).to eq(5)
      expect(task.queue("cops").nodes.where(kind: "head").first.nodable).to eq(edward)
      any = task.queue("cops").nodes.where(kind: "any").map(&:nodable)
      expect(any).to include(cindy)
      expect(any).to include(david)
      expect(any).to include(bob)
      expect(task.queue("cops").nodes.where(kind: "tail").first.nodable).to eq(frank)

      result = task.queue("cops").shift(
        sort_exp: ->(u) { handle_time[u.name] },
        sort_order: :desc
      )
      expect(result.name).to eq("Edward")
      expect(task.queue("cops").nodes.count).to eq(4)
      expect(task.queue("cops").count_of_nodes).to eq(4)
      expect(task.queue("cops").nodes.where(kind: "head").first.nodable).to eq(bob)
      any = task.queue("cops").nodes.where(kind: "any").map(&:nodable)
      expect(any).to include(cindy)
      expect(any).to include(david)
      expect(task.queue("cops").nodes.where(kind: "tail").first.nodable).to eq(frank)
    end

    it "should return the first node in the queue sorted by field" do
      john = create(:user, name: "John")
      bob = create(:user, name: "Bob")
      task.queue("cops")
          .push(john)
          .push(bob)

      expect(task.queue("cops").nodes.count).to eq(2)
      expect(task.queue("cops").count_of_nodes).to eq(2)
      expect(task.queue("cops").nodes.where(kind: "head").first.nodable).to eq(john)
      expect(task.queue("cops").nodes.where(kind: "tail").first.nodable).to eq(bob)

      result = task.queue("cops").shift(
        sort_exp: :name,
        sort_order: :desc
      )
      expect(result.name).to eq("John")
      expect(task.queue("cops").nodes.count).to eq(1)
      expect(task.queue("cops").count_of_nodes).to eq(1)
      expect(task.queue("cops").nodes.where(kind: "head").first.nodable).to eq(bob)
    end

    it "should sort the queue by the given sort expression and order and return the last value" do
      eric = create(:user, name: "Eric")
      xia = create(:user, name: "Xia")
      john = create(:user, name: "John")
      task.queue("cops")
          .push(eric)
          .push(xia)
          .push(john)

      dobs = {
        "Xia" => Date.new(1992, 1, 1),
        "Eric" => Date.new(1990, 1, 1),
        "John" => Date.new(1991, 1, 1)
      }

      expect(task.queue("cops").nodes.count).to eq(3)
      expect(task.queue("cops").count_of_nodes).to eq(3)
      expect(task.queue("cops").nodes.where(kind: "head").first.nodable).to eq(eric)
      expect(task.queue("cops").nodes.where(kind: "any").first.nodable).to eq(xia)
      expect(task.queue("cops").nodes.where(kind: "tail").first.nodable).to eq(john)

      result = task.queue("cops").shift(
        sort_exp: ->(u) { dobs[u.name] },
        sort_order: :asc
      )
      expect(result.name).to eq("Eric")
      expect(task.queue("cops").nodes.count).to eq(2)
      expect(task.queue("cops").count_of_nodes).to eq(2)
      expect(task.queue("cops").nodes.where(kind: "head").first.nodable).to eq(xia)
      expect(task.queue("cops").nodes.where(kind: "tail").first.nodable).to eq(john)
    end

    it "should return the first node in the queue that match the given filter expression" do
      bob = create(:user, name: "Bob")
      xia = create(:user, name: "Xia")
      john = create(:user, name: "John")
      task.queue("cops")
          .push(bob)
          .push(xia)
          .push(john)

      result = task.queue("cops").shift(
        filter_exp: ->(u) { u.name != "John" }
      )
      expect(result.name).to eq("Bob")
      expect(task.queue("cops").nodes.count).to eq(2)
      expect(task.queue("cops").count_of_nodes).to eq(2)
      expect(task.queue("cops").nodes.where(kind: "head").first.nodable).to eq(xia)
      expect(task.queue("cops").nodes.where(kind: "tail").first.nodable).to eq(john)
    end

    context "when there are 2 nodes in the queue" do
      it "should mark the remaining item as the head node" do
        john = create(:user, name: "John")
        bob = create(:user, name: "Bob")
        task.queue("cops")
            .push(john)
            .push(bob)

        expect(task.queue("cops").nodes.count).to eq(2)
        expect(task.queue("cops").count_of_nodes).to eq(2)
        expect(task.queue("cops").nodes.where(kind: "head").first.nodable).to eq(john)
        expect(task.queue("cops").nodes.where(kind: "tail").first.nodable).to eq(bob)

        task.queue("cops").shift
        expect(task.queue("cops").nodes.count).to eq(1)
        expect(task.queue("cops").count_of_nodes).to eq(1)
        expect(task.queue("cops").nodes.where(kind: "head").first.nodable).to eq(bob)
      end
    end

    context "when there are 3 nodes in the queue" do
      it "should mark the remaining item as the head node" do
        john = create(:user, name: "John")
        bob = create(:user, name: "Bob")
        cindy = create(:user, name: "Cindy")
        task.queue("cops")
            .push(john)
            .push(bob)
            .push(cindy)

        expect(task.queue("cops").nodes.count).to eq(3)
        expect(task.queue("cops").count_of_nodes).to eq(3)
        expect(task.queue("cops").nodes.where(kind: "head").first.nodable).to eq(john)
        expect(task.queue("cops").nodes.where(kind: "any").first.nodable).to eq(bob)
        expect(task.queue("cops").nodes.where(kind: "tail").first.nodable).to eq(cindy)

        task.queue("cops").shift
        expect(task.queue("cops").nodes.count).to eq(2)
        expect(task.queue("cops").count_of_nodes).to eq(2)
        expect(task.queue("cops").nodes.where(kind: "head").first.nodable).to eq(bob)
        expect(task.queue("cops").nodes.where(kind: "tail").first.nodable).to eq(cindy)
      end
    end

    context "when there are 4 nodes in the queue" do
      it "should mark the remaining item as the head node" do
        john = create(:user, name: "John")
        bob = create(:user, name: "Bob")
        cindy = create(:user, name: "Cindy")
        david = create(:user, name: "David")
        task.queue("cops")
            .push(john)
            .push(bob)
            .push(cindy)
            .push(david)

        expect(task.queue("cops").nodes.count).to eq(4)
        expect(task.queue("cops").count_of_nodes).to eq(4)
        expect(task.queue("cops").nodes.where(kind: "head").first.nodable).to eq(john)
        any = task.queue("cops").nodes.where(kind: "any").map(&:nodable)
        expect(any).to include(bob)
        expect(any).to include(cindy)
        expect(task.queue("cops").nodes.where(kind: "tail").first.nodable).to eq(david)

        task.queue("cops").shift
        expect(task.queue("cops").nodes.count).to eq(3)
        expect(task.queue("cops").count_of_nodes).to eq(3)
        expect(task.queue("cops").nodes.where(kind: "head").first.nodable).to eq(bob)
        expect(task.queue("cops").nodes.where(kind: "any").first.nodable).to eq(cindy)
        expect(task.queue("cops").nodes.where(kind: "tail").first.nodable).to eq(david)
      end
    end
  end

  describe '#nodables' do
    it "returns the nodes in the queue sorted using the given sort expression and nodable order" do
      task.queue("cops").push(create(:user, name: "Edward"))
      task.queue("cops").push(create(:user, name: "Amy"))
      task.queue("cops").push(create(:user, name: "Bob"))
      task.queue("cops").push(create(:user, name: "David"))
      task.queue("cops").push(create(:user, name: "Cindy"))
      task.queue("cops").push(create(:user, name: "Frank"))

      handle_time = {
        "Amy" => 0,
        "Bob" => 0,
        "David" => 1,
        "Cindy" => 0,
        "Edward" => 2,
        "Frank" => 0
      }

      result = task.queue("cops").nodables(
        sort_exp: ->(u) { handle_time[u.name] },
        sort_order: :asc
      )
      expect(result.map(&:name)).to eq(%w(Amy Bob Cindy Frank David Edward))

      result = task.queue("cops").nodables(
        sort_exp: ->(u) { handle_time[u.name] },
        sort_order: :desc
      )
      expect(result.map(&:name)).to eq(%w(Edward David Frank Cindy Bob Amy))
    end

    it "returns the nodes in the queue sorted using the given sort expression and sort order" do
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

    it "returns the nodes in the queue that match the given filter expression and sorted \
      using the given sort expression and sort order" do
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

    it "returns the nodes in the order they were added to the queue if no sort expression \
      is given" do
      task.queue("cops").push(create(:user, name: "Bob"))
      task.queue("cops").push(create(:user, name: "Xia"))
      task.queue("cops").unshift(create(:user, name: "John"))

      result = task.queue("cops").nodables
      expect(result.map(&:name)).to eq(%w(John Bob Xia))
    end
  end

  describe '#peek' do
    it "should return the first node in the queue sorted by field and nodable order" do
      task.queue("cops").push(create(:user, name: "Edward"))
      task.queue("cops").push(create(:user, name: "Amy"))
      task.queue("cops").push(create(:user, name: "Bob"))
      task.queue("cops").push(create(:user, name: "David"))
      task.queue("cops").push(create(:user, name: "Cindy"))
      task.queue("cops").push(create(:user, name: "Frank"))

      handle_time = {
        "Amy" => 0,
        "Bob" => 0,
        "David" => 1,
        "Cindy" => 0,
        "Edward" => 2,
        "Frank" => 0
      }

      result = task.queue("cops").peek(
        sort_exp: ->(u) { handle_time[u.name] },
        sort_order: :asc
      )
      expect(result.name).to eq("Amy")
      expect(task.queue("cops").nodes.count).to eq(6)
      expect(task.queue("cops").count_of_nodes).to eq(6)
    end

    it "should return the first node in the queue sorted by field" do
      task.queue("cops").push(create(:user, name: "John"))
      task.queue("cops").push(create(:user, name: "Bob"))

      result = task.queue("cops").peek(
        sort_exp: :name,
        sort_order: :desc
      )
      expect(result.name).to eq("John")
      expect(task.queue("cops").nodes.count).to eq(2)
      expect(task.queue("cops").count_of_nodes).to eq(2)
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
    it "should not trigger callback if none were removed" do
      john = create(:user, name: "John")
      bob = create(:user, name: "Bob")
      task.queue("cops").push(john)

      queue_callback = double("QueueCallback")
      expect(queue_callback).to receive(:call).never
      expect(QueueIt).to receive(:queue_callback).and_return(queue_callback).never

      task.queue("cops").remove(bob)
    end

    it "should remove the nodable from the queue" do
      john = create(:user, name: "John")
      bob = create(:user, name: "Bob")
      task.queue("cops")
          .push(john)
          .push(bob)

      task.queue("cops").remove(john)
      expect(task.queue("cops").nodes.count).to eq(1)
      expect(task.queue("cops").count_of_nodes).to eq(1)
    end

    it "should emit callback" do
      john = create(:user, name: "John")
      bob = create(:user, name: "Bob")
      task.queue("cops")
          .push(john)
          .push(bob)

      queue_callback = double("QueueCallback")
      expect(queue_callback).to receive(:call).once
      expect(QueueIt).to receive(:queue_callback).and_return(queue_callback).twice
      task.queue("cops").remove(john)
      expect(task.queue("cops").nodes.count).to eq(1)
      expect(task.queue("cops").count_of_nodes).to eq(1)
    end

    context "when there are 2 nodes in the queue and remove on the first item" do
      it "should leave remaining node as kind as head" do
        john = create(:user, name: "John")
        bob = create(:user, name: "Bob")
        task.queue("cops")
            .push(john)
            .push(bob)

        expect(task.queue("cops").nodes.count).to eq(2)
        expect(task.queue("cops").count_of_nodes).to eq(2)
        expect(task.queue("cops").nodes.where(kind: "head").first.nodable).to eq(john)
        expect(task.queue("cops").nodes.where(kind: "tail").first.nodable).to eq(bob)

        task.queue("cops").remove(john)
        expect(task.queue("cops").nodes.count).to eq(1)
        expect(task.queue("cops").count_of_nodes).to eq(1)
        expect(task.queue("cops").nodes.where(kind: "head").first.nodable).to eq(bob)
      end
    end

    context "when there are 2 nodes in the queue and remove on the last item" do
      it "should leave remaining node as kind as head" do
        john = create(:user, name: "John")
        bob = create(:user, name: "Bob")
        task.queue("cops")
            .push(john)
            .push(bob)

        expect(task.queue("cops").nodes.count).to eq(2)
        expect(task.queue("cops").count_of_nodes).to eq(2)
        expect(task.queue("cops").nodes.where(kind: "head").first.nodable).to eq(john)
        expect(task.queue("cops").nodes.where(kind: "tail").first.nodable).to eq(bob)

        task.queue("cops").remove(bob)
        expect(task.queue("cops").nodes.count).to eq(1)
        expect(task.queue("cops").count_of_nodes).to eq(1)
        expect(task.queue("cops").nodes.where(kind: "head").first.nodable).to eq(john)
      end
    end

    context "when there are 3 nodes in the queue and remove on the first item" do
      it "should leave remaining node as kind as head" do
        john = create(:user, name: "John")
        bob = create(:user, name: "Bob")
        cindy = create(:user, name: "Cindy")
        task.queue("cops")
            .push(john)
            .push(bob)
            .push(cindy)

        expect(task.queue("cops").nodes.count).to eq(3)
        expect(task.queue("cops").count_of_nodes).to eq(3)
        expect(task.queue("cops").nodes.where(kind: "head").first.nodable).to eq(john)
        expect(task.queue("cops").nodes.where(kind: "any").first.nodable).to eq(bob)
        expect(task.queue("cops").nodes.where(kind: "tail").first.nodable).to eq(cindy)

        task.queue("cops").remove(john)
        expect(task.queue("cops").nodes.count).to eq(2)
        expect(task.queue("cops").count_of_nodes).to eq(2)
        expect(task.queue("cops").nodes.where(kind: "head").first.nodable).to eq(bob)
        expect(task.queue("cops").nodes.where(kind: "tail").first.nodable).to eq(cindy)
      end
    end

    context "when there are 3 nodes in the queue and remove on the last item" do
      it "should leave remaining node as kind as head" do
        john = create(:user, name: "John")
        bob = create(:user, name: "Bob")
        cindy = create(:user, name: "Cindy")
        task.queue("cops")
            .push(john)
            .push(bob)
            .push(cindy)

        expect(task.queue("cops").nodes.count).to eq(3)
        expect(task.queue("cops").count_of_nodes).to eq(3)
        expect(task.queue("cops").nodes.where(kind: "head").first.nodable).to eq(john)
        expect(task.queue("cops").nodes.where(kind: "any").first.nodable).to eq(bob)
        expect(task.queue("cops").nodes.where(kind: "tail").first.nodable).to eq(cindy)

        task.queue("cops").remove(cindy)
        expect(task.queue("cops").nodes.count).to eq(2)
        expect(task.queue("cops").count_of_nodes).to eq(2)
        expect(task.queue("cops").nodes.where(kind: "head").first.nodable).to eq(john)
        expect(task.queue("cops").nodes.where(kind: "tail").first.nodable).to eq(bob)
      end
    end

    context "when there are 3 nodes in the queue and remove on the middle item" do
      it "should leave remaining node as kind as head" do
        john = create(:user, name: "John")
        bob = create(:user, name: "Bob")
        cindy = create(:user, name: "Cindy")
        task.queue("cops")
            .push(john)
            .push(bob)
            .push(cindy)

        expect(task.queue("cops").nodes.count).to eq(3)
        expect(task.queue("cops").count_of_nodes).to eq(3)
        expect(task.queue("cops").nodes.where(kind: "head").first.nodable).to eq(john)
        expect(task.queue("cops").nodes.where(kind: "any").first.nodable).to eq(bob)
        expect(task.queue("cops").nodes.where(kind: "tail").first.nodable).to eq(cindy)

        task.queue("cops").remove(bob)
        expect(task.queue("cops").nodes.count).to eq(2)
        expect(task.queue("cops").count_of_nodes).to eq(2)
        expect(task.queue("cops").nodes.where(kind: "head").first.nodable).to eq(john)
        expect(task.queue("cops").nodes.where(kind: "tail").first.nodable).to eq(cindy)
      end
    end

    context "when there are 4 nodes in the queue and remove on the first item" do
      it "should leave remaining node as kind as head" do
        john = create(:user, name: "John")
        bob = create(:user, name: "Bob")
        cindy = create(:user, name: "Cindy")
        david = create(:user, name: "David")
        task.queue("cops")
            .push(john)
            .push(bob)
            .push(cindy)
            .push(david)

        expect(task.queue("cops").nodes.count).to eq(4)
        expect(task.queue("cops").count_of_nodes).to eq(4)
        expect(task.queue("cops").nodes.where(kind: "tail").first.nodable).to eq(david)
        any = task.queue("cops").nodes.where(kind: "any").map(&:nodable)
        expect(any).to include(bob)
        expect(any).to include(cindy)
        expect(task.queue("cops").nodes.where(kind: "head").first.nodable).to eq(john)

        task.queue("cops").remove(john)
        expect(task.queue("cops").nodes.count).to eq(3)
        expect(task.queue("cops").count_of_nodes).to eq(3)

        expect(task.queue("cops").nodes.where(kind: "head").first.nodable).to eq(bob)
        expect(task.queue("cops").nodes.where(kind: "any").first.nodable).to eq(cindy)
        expect(task.queue("cops").nodes.where(kind: "tail").first.nodable).to eq(david)
      end
    end

    context "when there are 4 nodes in the queue and remove on the last item" do
      it "should leave remaining node as kind as head" do
        john = create(:user, name: "John")
        bob = create(:user, name: "Bob")
        cindy = create(:user, name: "Cindy")
        david = create(:user, name: "David")
        task.queue("cops")
            .push(john)
            .push(bob)
            .push(cindy)
            .push(david)

        expect(task.queue("cops").nodes.count).to eq(4)
        expect(task.queue("cops").count_of_nodes).to eq(4)

        expect(task.queue("cops").nodes.where(kind: "head").first.nodable).to eq(john)
        any = task.queue("cops").nodes.where(kind: "any").map(&:nodable)
        expect(any).to include(bob)
        expect(any).to include(cindy)
        expect(task.queue("cops").nodes.where(kind: "tail").first.nodable).to eq(david)

        task.queue("cops").remove(david)
        expect(task.queue("cops").nodes.count).to eq(3)
        expect(task.queue("cops").count_of_nodes).to eq(3)
        expect(task.queue("cops").nodes.where(kind: "head").first.nodable).to eq(john)
        expect(task.queue("cops").nodes.where(kind: "any").first.nodable).to eq(bob)
        expect(task.queue("cops").nodes.where(kind: "tail").first.nodable).to eq(cindy)
      end
    end

    context "when there are 4 nodes in the queue and remove on the second item" do
      it "should leave remaining node as kind as head" do
        john = create(:user, name: "John")
        bob = create(:user, name: "Bob")
        cindy = create(:user, name: "Cindy")
        david = create(:user, name: "David")
        task.queue("cops")
            .push(john)
            .push(bob)
            .push(cindy)
            .push(david)

        expect(task.queue("cops").nodes.count).to eq(4)
        expect(task.queue("cops").count_of_nodes).to eq(4)
        expect(task.queue("cops").nodes.where(kind: "head").first.nodable).to eq(john)
        any = task.queue("cops").nodes.where(kind: "any").map(&:nodable)
        expect(any).to include(bob)
        expect(any).to include(cindy)
        expect(task.queue("cops").nodes.where(kind: "tail").first.nodable).to eq(david)

        task.queue("cops").remove(bob)
        expect(task.queue("cops").nodes.count).to eq(3)
        expect(task.queue("cops").count_of_nodes).to eq(3)
        expect(task.queue("cops").nodes.where(kind: "head").first.nodable).to eq(john)
        expect(task.queue("cops").nodes.where(kind: "any").first.nodable).to eq(cindy)
        expect(task.queue("cops").nodes.where(kind: "tail").first.nodable).to eq(david)
      end
    end

    context "when there are 4 nodes in the queue and remove on the third item" do
      it "should leave remaining node as kind as head" do
        john = create(:user, name: "John")
        bob = create(:user, name: "Bob")
        cindy = create(:user, name: "Cindy")
        david = create(:user, name: "David")
        task.queue("cops")
            .push(john)
            .push(bob)
            .push(cindy)
            .push(david)

        expect(task.queue("cops").nodes.count).to eq(4)
        expect(task.queue("cops").count_of_nodes).to eq(4)
        expect(task.queue("cops").nodes.where(kind: "head").first.nodable).to eq(john)
        any = task.queue("cops").nodes.where(kind: "any").map(&:nodable)
        expect(any).to include(bob)
        expect(any).to include(cindy)
        expect(task.queue("cops").nodes.where(kind: "tail").first.nodable).to eq(david)

        task.queue("cops").remove(cindy)
        expect(task.queue("cops").nodes.count).to eq(3)
        expect(task.queue("cops").count_of_nodes).to eq(3)
        expect(task.queue("cops").nodes.where(kind: "head").first.nodable).to eq(john)
        expect(task.queue("cops").nodes.where(kind: "any").first.nodable).to eq(bob)
        expect(task.queue("cops").nodes.where(kind: "tail").first.nodable).to eq(david)
      end
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
    it "does not call queue_callback when queue is modified inside the suppress_callbacks block" do
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
        queue.push(create(:user, name: "John"))
      end
    end
  end

  describe '#find_or_create_queue!' do
    it "should return the queue with the given name" do
      queue = task.find_or_create_queue!("cops")
      expect(queue.name).to eq("cops")
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
