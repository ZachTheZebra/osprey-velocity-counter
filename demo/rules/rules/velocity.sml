Import(rules=['models/base.sml'])

RecentMessage = Rule(
  when_all=[UserMessages >= 1],
  description='The user has a message inside the window.'
)
