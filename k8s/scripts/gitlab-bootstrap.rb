# Idempotent GitLab bootstrap for the NiFi lab; run with `gitlab-rails runner`.
# Called by k8s/scripts/gitlab-bootstrap.py, which passes LAB_TOKENS and stores
# the returned token values in Vault. Prints one "LAB_RESULT <json>" line.
#
# LAB_TOKENS: JSON list of {name, access_level, scopes, days, revoke_only}
#   Each named Project Access Token is revoked (if active) and, unless
#   revoke_only, created again. Values exist only in this run's output.
require 'json'

GROUP_PATH = ENV.fetch('LAB_GROUP', 'data360')
PROJECT_PATH = ENV.fetch('LAB_PROJECT', 'flows-nifi')

def unwrap(result, key)
  object = result.is_a?(ServiceResponse) ? result.payload[key] : result
  return object if object.respond_to?(:persisted?) && object.persisted?

  details = object.respond_to?(:errors) ? object.errors.full_messages : []
  message = result.respond_to?(:message) ? result.message : nil
  raise "#{key} failed: #{[message, *details].compact.join('; ')}"
end

root = User.find_by_username!('root')
# GitLab 17+ places every top-level group in an organization.
organization = Organizations::Organization.find_by(path: 'default') || root.organizations.first
steps = []

group = Group.find_by_full_path(GROUP_PATH)
unless group
  group = unwrap(Groups::CreateService.new(root, name: GROUP_PATH, path: GROUP_PATH, organization_id: organization.id,
                                           visibility_level: Gitlab::VisibilityLevel::PRIVATE).execute, :group)
  steps << "created group #{GROUP_PATH}"
end

project = Project.find_by_full_path("#{GROUP_PATH}/#{PROJECT_PATH}")
if project && ENV['LAB_RECREATE_PROJECT'] == '1'
  # Removes snapshots, MRs and the project's bot users/tokens (lab reset).
  raise "destroy #{project.full_path} failed" unless ::Projects::DestroyService.new(project, root, {}).execute

  steps << "destroyed project #{project.full_path}"
  project = nil
end
unless project
  project = unwrap(Projects::CreateService.new(root, name: PROJECT_PATH, path: PROJECT_PATH, namespace_id: group.id,
                                               visibility_level: Gitlab::VisibilityLevel::PRIVATE,
                                               initialize_with_readme: true, default_branch: 'main').execute, :project)
  steps << "created project #{project.full_path}"
end

repo = project.repository
unless repo.branch_exists?('main')
  result = Branches::CreateService.new(project, root).execute('main', repo.root_ref)
  raise "main branch: #{result[:message]}" unless result[:status] == :success

  steps << 'created branch main'
end
if project.default_branch != 'main'
  project.change_head('main')
  steps << 'default branch -> main'
end

# Must happen before main is protected with "No one" allowed to push.
unless repo.blob_at('main', 'flows/shared/.keep')
  result = Files::CreateService.new(project, root, start_branch: 'main', branch_name: 'main',
                                    commit_message: 'Initialize shared flow bucket',
                                    file_path: 'flows/shared/.keep', file_content: '').execute
  raise "flows/shared/.keep: #{result[:message]}" unless result[:status] == :success

  steps << 'committed flows/shared/.keep on main'
end

unless repo.branch_exists?('staging')
  result = Branches::CreateService.new(project, root).execute('staging', 'main')
  raise "staging branch: #{result[:message]}" unless result[:status] == :success

  steps << 'created branch staging'
end

protected_main = project.protected_branches.find_by(name: 'main')
wanted = protected_main &&
         protected_main.push_access_levels.map(&:access_level) == [Gitlab::Access::NO_ACCESS] &&
         protected_main.merge_access_levels.map(&:access_level) == [Gitlab::Access::MAINTAINER] &&
         !protected_main.allow_force_push
unless wanted
  ProtectedBranches::DestroyService.new(project, root).execute(protected_main) if protected_main
  created = ProtectedBranches::CreateService.new(project, root, name: 'main', allow_force_push: false,
                                                 push_access_levels_attributes: [{ access_level: Gitlab::Access::NO_ACCESS }],
                                                 merge_access_levels_attributes: [{ access_level: Gitlab::Access::MAINTAINER }]).execute
  raise "protect main: #{created.errors.full_messages}" unless created.persisted?

  steps << 'protected main: merge=Maintainers, push=No one, no force push'
end

project.update!(merge_method: :merge, remove_source_branch_after_merge: false)
project.project_setting.update!(squash_option: :never) if project.project_setting.squash_option.to_s != 'never'

bot_ids = project.project_members.joins(:user).where(users: { user_type: User.user_types[:project_bot] }).pluck(:user_id)
tokens = []
JSON.parse(ENV.fetch('LAB_TOKENS', '[]')).each do |spec|
  PersonalAccessToken.where(user_id: bot_ids, name: spec['name']).active.each do |old|
    ResourceAccessTokens::RevokeService.new(root, project, old).execute
    steps << "revoked project access token #{spec['name']} (id #{old.id})"
  end
  next if spec['revoke_only']

  params = { name: spec['name'], scopes: spec['scopes'].map(&:to_sym), access_level: spec['access_level'],
             expires_at: Date.current + spec.fetch('days', 180) }
  result = ResourceAccessTokens::CreateService.new(root, project, params).execute
  raise "token #{spec['name']}: #{result.message}" unless result.success?

  token = result.payload[:access_token]
  tokens << { name: token.name, id: token.id, token: token.token, bot_user: token.user.username,
              scopes: token.scopes.map(&:to_s), access_level: spec['access_level'], expires_at: token.expires_at.to_s }
  steps << "created project access token #{token.name} (id #{token.id}, bot #{token.user.username})"
end

puts 'LAB_RESULT ' + JSON.generate(project_id: project.id, project: project.full_path, web_url: project.web_url,
                                   default_branch: project.default_branch,
                                   branches: repo.branch_names.sort, steps: steps, tokens: tokens)
