# app.rb (VERSÃO FINAL CORRIGIDA)

require 'sinatra/base'
require 'pg'
require 'connection_pool'
require 'json'
require 'puma'
require 'httparty'
require 'redis'

class RinhaAPI < Sinatra::Base # <-- CORRIGIDO AQUI
  configure do
    set :server, :puma
    set :bind, '0.0.0.0'
    set :port, 3000
    set :environment, :production
  end

  DB_POOL = ConnectionPool.new(size: 25, timeout: 5) do
    PG.connect(
      host: 'db', user: 'rinha', password: 'rinha-password', dbname: 'rinha_db'
    )
  end

  REDIS_URL = ENV.fetch('REDIS_URL', 'redis://localhost:6379/0')
  REDIS = Redis.new(url: REDIS_URL)
  
  PROCESSORS = {
    default: 'http://payment-processor-default:8080',
    fallback: 'http://payment-processor-fallback:8080'
  }
  HEALTH_CHECK_INTERVAL = 5

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
    begin
      content_type :json
      from_param = params['from']
      to_param = params['to']
      from_time = from_param ? Time.parse(from_param).utc : nil
      to_time = to_param ? Time.parse(to_param).utc : nil
      query, query_params = build_summary_query(from_time, to_time)
      result = DB_POOL.with { |conn| conn.exec_params(query, query_params).first }
      unless result
        halt 200, { default: { totalRequests: 0, totalAmount: 0.0 }, fallback: { totalRequests: 0, totalAmount: 0.0 } }.to_json
      end
      {
        default: { totalRequests: result['default_requests'].to_i, totalAmount: result['default_amount'].to_f },
        fallback: { totalRequests: result['fallback_requests'].to_i, totalAmount: result['fallback_amount'].to_f }
      }.to_json
    rescue => e
      logger.error "ERRO FATAL NO SUMMARY: #{e.class} - #{e.message}"
      halt 500, { error: 'Erro interno ao processar o resumo' }.to_json
    end
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
          save_payment(correlation_id, amount, processor_name.to_s, request_time.strftime('%Y-%m-%d %H:%M:%S.%N'))
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