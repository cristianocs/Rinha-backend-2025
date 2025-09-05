require 'sinatra/base'
require 'json'
require 'puma'
require 'httparty'
require 'redis'

class RinhaAPI < Sinatra::Base
  configure do
    set :server, :puma
    set :bind, '0.0.0.0'
    set :port, 3000
    set :environment, :production
  end

  REDIS_URL = ENV.fetch('REDIS_URL', 'redis://redis:6379/1')
  REDIS = Redis.new(url: REDIS_URL)
  
  PROCESSORS = {
    default: 'http://payment-processor-default:8080',
    fallback: 'http://payment-processor-fallback:8080'
  }
  HEALTH_CHECK_INTERVAL = 5
  PAYMENTS_KEY = "payments_sorted_set"

  post '/payments' do
    content_type :json
    begin
      req = JSON.parse(request.body.read)
      correlation_id = req['correlationId']
      amount = req['amount']
      halt 400, { error: 'Invalid payload' }.to_json unless correlation_id && amount
      process_payment(correlation_id, amount)
      status 202
      { message: 'Payment processing started' }.to_json
    rescue JSON::ParserError
      halt 400, { error: 'Invalid JSON' }.to_json
    end
  end

  get '/payments-summary' do
    content_type :json
    from_param = params['from']
    to_param = params['to']
    
    from_time = from_param ? (Time.parse(from_param).to_f * 1_000_000).to_i : '-inf'
    to_time = to_param ? (Time.parse(to_param).to_f * 1_000_000).to_i : '+inf'

    payments = REDIS.zrangebyscore(PAYMENTS_KEY, from_time, to_time)

    summary = {
      default: { totalRequests: 0, totalAmount: 0.0 },
      fallback: { totalRequests: 0, totalAmount: 0.0 }
    }

    payments.each do |payment_str|
      amount, processor, _ = payment_str.split(':', 3)
      processor_sym = processor.to_sym
      summary[processor_sym][:totalRequests] += 1
      summary[processor_sym][:totalAmount] += amount.to_f
    end

    summary.to_json
  end

  private

  def process_payment(correlation_id, amount)
    processor_name = choose_processor
    request_time = Time.now.utc 
    
    Thread.new do
      begin
        response = HTTParty.post(
          "#{PROCESSORS[processor_name]}/payments",
          body: { 
            correlationId: correlation_id, 
            amount: amount, 
            requestedAt: request_time.iso8601(3) 
          }.to_json,
          headers: { 'Content-Type' => 'application/json' },
          timeout: 2
        )
        if response.success?
          save_payment_in_memory(correlation_id, amount, processor_name.to_s)
        else
          set_processor_health(processor_name, true)
        end
      rescue => e
        logger.error "Erro na thread de pagamento: #{e.message}"
        set_processor_health(processor_name, true)
      end
    end
  end

  def save_payment(correlation_id, amount, processor, formatted_time)
    sql = "INSERT INTO payments (correlation_id, amount, processor, requested_at) VALUES ($1, $2, $3, $4)"
    DB_POOL.with do |conn|
      conn.exec_params(sql, [correlation_id, amount, processor, formatted_time])
    end
  end
  
  def save_payment_in_memory(correlation_id, amount, processor)
    score = (Time.now.to_f * 1_000_000).to_i
    member = "#{amount}:#{processor}:#{correlation_id}"
    
    REDIS.zadd(PAYMENTS_KEY, score, member)
  end

  def choose_processor
    return :default unless processor_failing?(:default)
    check_health_if_needed(:fallback)
    return :fallback unless processor_failing?(:fallback)
    clear_processor_health(:default)
    :default
  end

  def processor_failing?(processor)
    REDIS.exists?("health:#{processor}:failing")
  end
  
  def clear_processor_health(processor)
     REDIS.del("health:#{processor}:failing")
  end

  def check_health_if_needed(processor)
    return unless REDIS.set("health:#{processor}:last_checked", "1", nx: true, ex: HEALTH_CHECK_INTERVAL)
    check_health(processor)
  end

  def check_health(processor)
    url = "#{PROCESSORS[processor]}/payments/service-health"
    is_failing = false
    begin
      response = HTTParty.get(url, timeout: 1)
      is_failing = !response.success? || JSON.parse(response.body)['failing']
    rescue
      is_failing = true
    end
    set_processor_health(processor, is_failing)
  end

  def set_processor_health(processor, is_failing)
    key = "health:#{processor}:failing"
    if is_failing
      REDIS.setex(key, HEALTH_CHECK_INTERVAL * 3, "true")
    else
      REDIS.del(key)
    end
  end

  def build_summary_query(from_time, to_time)
    base_query = <<-SQL
      WITH filtered_payments AS (
        SELECT processor, amount FROM payments
        WHERE
          ($1::TIMESTAMPTZ IS NULL OR requested_at >= $1::TIMESTAMPTZ)
          AND
          ($2::TIMESTAMPTZ IS NULL OR requested_at <= $2::TIMESTAMPTZ)
      )
      SELECT
        COALESCE(SUM(CASE WHEN processor = 'default' THEN 1 ELSE 0 END), 0) AS default_requests,
        COALESCE(SUM(CASE WHEN processor = 'default' THEN amount ELSE 0 END), 0) AS default_amount,
        COALESCE(SUM(CASE WHEN processor = 'fallback' THEN 1 ELSE 0 END), 0) AS fallback_requests,
        COALESCE(SUM(CASE WHEN processor = 'fallback' THEN amount ELSE 0 END), 0) AS fallback_amount
      FROM filtered_payments
    SQL
    params = [from_time, to_time]
    return [base_query, params]
  end
end