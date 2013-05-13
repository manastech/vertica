# Generated by jeweler
# DO NOT EDIT THIS FILE DIRECTLY
# Instead, edit Jeweler::Tasks in Rakefile, and run 'rake gemspec'
# -*- encoding: utf-8 -*-

Gem::Specification.new do |s|
  s.name = "vertica"
  s.version = "0.10.1"

  s.required_rubygems_version = Gem::Requirement.new(">= 0") if s.respond_to? :required_rubygems_version=
  s.authors = ["Jeff Smick", "Matt Bauer", "Willem van Bergen"]
  s.date = "2013-04-12"
  s.description = "Query Vertica with ruby"
  s.email = "sprsquish@gmail.com"
  s.extra_rdoc_files = [
    "LICENSE",
    "README.md"
  ]
  s.files = [
    "Gemfile",
    "Gemfile.lock",
    "LICENSE",
    "README.md",
    "Rakefile",
    "VERSION",
    "ext/vertica/extconf.rb",
    "ext/vertica/vertica.c",
    "lib/vertica.rb",
    "lib/vertica/column.rb",
    "lib/vertica/connection.rb",
    "lib/vertica/error.rb",
    "lib/vertica/messages/backend_messages/authentication.rb",
    "lib/vertica/messages/backend_messages/backend_key_data.rb",
    "lib/vertica/messages/backend_messages/bind_complete.rb",
    "lib/vertica/messages/backend_messages/close_complete.rb",
    "lib/vertica/messages/backend_messages/command_complete.rb",
    "lib/vertica/messages/backend_messages/copy_in_response.rb",
    "lib/vertica/messages/backend_messages/data_row.rb",
    "lib/vertica/messages/backend_messages/empty_query_response.rb",
    "lib/vertica/messages/backend_messages/error_response.rb",
    "lib/vertica/messages/backend_messages/no_data.rb",
    "lib/vertica/messages/backend_messages/notice_response.rb",
    "lib/vertica/messages/backend_messages/parameter_description.rb",
    "lib/vertica/messages/backend_messages/parameter_status.rb",
    "lib/vertica/messages/backend_messages/parse_complete.rb",
    "lib/vertica/messages/backend_messages/portal_suspended.rb",
    "lib/vertica/messages/backend_messages/ready_for_query.rb",
    "lib/vertica/messages/backend_messages/row_description.rb",
    "lib/vertica/messages/backend_messages/unknown.rb",
    "lib/vertica/messages/frontend_messages/bind.rb",
    "lib/vertica/messages/frontend_messages/cancel_request.rb",
    "lib/vertica/messages/frontend_messages/close.rb",
    "lib/vertica/messages/frontend_messages/copy_data.rb",
    "lib/vertica/messages/frontend_messages/copy_done.rb",
    "lib/vertica/messages/frontend_messages/copy_fail.rb",
    "lib/vertica/messages/frontend_messages/describe.rb",
    "lib/vertica/messages/frontend_messages/execute.rb",
    "lib/vertica/messages/frontend_messages/flush.rb",
    "lib/vertica/messages/frontend_messages/parse.rb",
    "lib/vertica/messages/frontend_messages/password.rb",
    "lib/vertica/messages/frontend_messages/query.rb",
    "lib/vertica/messages/frontend_messages/ssl_request.rb",
    "lib/vertica/messages/frontend_messages/startup.rb",
    "lib/vertica/messages/frontend_messages/sync.rb",
    "lib/vertica/messages/frontend_messages/terminate.rb",
    "lib/vertica/messages/message.rb",
    "lib/vertica/query.rb",
    "lib/vertica/result.rb"
  ]
  s.extensions = ['ext/vertica/extconf.rb']
  s.homepage = "http://github.com/sprsquish/vertica"
  s.require_paths = ["lib"]
  s.rubygems_version = "1.8.25"
  s.summary = "Pure ruby library for interacting with Vertica"
  s.test_files = ["test/functional/connection_test.rb", "test/functional/query_test.rb", "test/functional/value_conversion_test.rb", "test/test_helper.rb", "test/unit/backend_message_test.rb", "test/unit/frontend_message_test.rb", "test/unit/quoting_test.rb"]

  if s.respond_to? :specification_version then
    s.specification_version = 3

    if Gem::Version.new(Gem::VERSION) >= Gem::Version.new('1.2.0') then
      s.add_runtime_dependency(%q<rake>, [">= 0"])
      s.add_runtime_dependency(%q<jeweler>, [">= 0"])
    else
      s.add_dependency(%q<rake>, [">= 0"])
      s.add_dependency(%q<jeweler>, [">= 0"])
    end
  else
    s.add_dependency(%q<rake>, [">= 0"])
    s.add_dependency(%q<jeweler>, [">= 0"])
  end
end

