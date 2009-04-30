module Nanite
  class AgentQueueLoop

    # FIX:
    # Two current issues:
    #
    #   1. Does not appear to execute processor_step when processing block is in progress, even if capacity is remaining -- need to look at use of EM.defer here, but further down in call pattern than here.
    #   2. Specs for Cluster::Route still fail -- they receive pop calls from this processor, seem to be left over from run of Agent spec.

    attr_accessor :should_exit

    def initialize(queue, capacity, retry_delay)
      @agent_mq = queue
      @capacity = capacity
      @accept_count = 0
      @mutex = Mutex.new
      @processing_block = nil
      @processor_steps_count = 0
      @retry_delay = retry_delay
      @should_exit = false
    end

    
    def accept_set_full_delay; @retry_delay; end
    def mq_empty_delay; @retry_delay; end
    
    def send_heartbeat?; accept_count_lt_cap?; end
    
    def start_processing(&blk)
      @processing_block = blk
      schedule_processor_step(0)
    end
    
    def processor_step
      decr_pending_processor_steps_count
      
      if accept_count_lt_cap?
        # can accept another job; incr accept_count in anticipation
        incr_accept_count

        # get next job off queue
        @agent_mq.pop do |info, msg|
        
          if msg
            # entry on mq: 
            #  1. call a block that:
            #    a. calls the processing block.
            #    b. decrements the accept count
            #    c. kicks the processor loop off again.
            EM.next_tick {
              @processing_block.call(info, msg, lambda { decr_accept_count }, lambda { schedule_processor_step(0) })
            }
          else
            # no entries waiting in mq
            decr_accept_count
            schedule_processor_step(mq_empty_delay)
          end
        end
      else
        # full.  should get here only in processor_step race conditions.
        if pending_processor_steps_count_gt_0?
          Nanite::Log.debug("Accept limit reached, and already have pending processor steps; no processor steps added.")
        else
          schedule_processor_step(accept_set_full_delay)
        end
      end
    end

    def incr_accept_count; x = @mutex.synchronize { @accept_count += 1 }; Nanite::Log.debug("incr_accept_count: #{x}"); x; end
    def decr_accept_count; x = @mutex.synchronize { @accept_count -= 1 }; Nanite::Log.debug("decr_accept_count: #{x}"); x; end
    def incr_pending_processor_steps_count; x = @mutex.synchronize { @processor_steps_count += 1 }; Nanite::Log.debug("incr_pending_processor_steps_count: #{x} curr accept_count: #{@accept_count}"); x; end
    def decr_pending_processor_steps_count; x = @mutex.synchronize { @processor_steps_count -= 1 }; Nanite::Log.debug("decr_pending_processor_steps_count: #{x} curr accept_count: #{@accept_count}"); x; end
    def pending_processor_steps_count_gt_0?; @mutex.synchronize { @processor_steps_count > 0 }; end
    
    def accept_count_lt_cap?; @mutex.synchronize { @accept_count < @capacity }; end
   
    def schedule_processor_step(delay=0)
      return nil if @should_exit
      incr_pending_processor_steps_count
      if delay && delay > 0
        EM.add_timer(delay) { self.processor_step }
      else
        EM.next_tick{ self.processor_step }
      end
    end
    
  end
end