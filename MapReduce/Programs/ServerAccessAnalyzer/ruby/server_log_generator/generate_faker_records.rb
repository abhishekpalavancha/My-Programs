require 'faker'

file = File.open("server_log.log", 'w')

(1..1000000).each do |x|
  file.write("#{Faker::Internet.ip_v4_address}\x04#{Faker::Time.between([Date.parse('2014-12-30'),
                                                                         Date.parse('2014-06-01'),
                                                                        Date.parse('2014-08-01')].sample,
                                                                        Date.parse('2014-01-01'),
                                                                        :all)}\x04#{Faker::Internet.user_name}\n")
end