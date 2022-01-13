from reaction.reaction import ExportPostsReactions

if __name__ == '__main__':
    names = ['efko7044']
    tokens = [
        'DQVJ1QlhMQVZALQ251LUhKZAER1Q1p6dllaWHp3TUJ4eUZAKT1ZAUeFZA6ajBBM2lwVnBIZAnZAjcDRNTlJ6bXNZAa1Bydkw2UUpTTDlBbUlkWGxzLUJBZAzZArOGJka3htUDhHcFZAVYzFpdXhHa213U2t0N0wySmZAOYk8tU3JwYnJKZAE8zbmw3b0FBNkhnYXUxcmhVa2x6aVBZAUGpqOTY0WjJva0h0ZA2RDMlVGX014TVJrRXA2NWhsYXBJS1BfZAnJBbmppYjNmUkxqcnNIWnpOWmx0VUUyeAZDZD',
    ]
    ed = ExportPostsReactions(names, tokens)

    # получаем посты
    for i in range(len(names)):
        ed.process(i)
