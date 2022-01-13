from pysql import DB_CONFIG, DB_commands, get_command, ex




def match_post_user():
    comm_post = "SELECT * FROM post"
    res_post = ex(DB_CONFIG, comm_post, 'v')
    for i in res_post:
        user_id_temp=i[-2]
        if i[0]!='c25f0014-47ed-4db4-aaab-23c0ca8af4d2':
            comm_user= "SELECT * FROM users WHERE user_id_temp=%s" % "'" + user_id_temp + "'"
            res_user = ex(DB_CONFIG, comm_user, 'v')
            for j in res_user:
                comm_update = "UPDATE post  SET user_id = %s WHERE user_id_temp =%s" % ("'" +j[0]+ "'","'" + user_id_temp + "'")
                ex(DB_CONFIG, comm_update)

def match_post_comment():
    comm_post = "SELECT * FROM post"
    res_post = ex(DB_CONFIG, comm_post, 'v')
    for i in res_post:
        id_post=i[0]
        id_post_temp=i[-1]
        comm_comments= "UPDATE comment SET post_id= %s WHERE post_id_temp=%s" % ("'" +id_post+ "'","'" + id_post_temp + "'")
        ex(DB_CONFIG, comm_comments)


def match_post_member():
    comm_user = "SELECT * FROM users"
    res_user = ex(DB_CONFIG, comm_user, 'v')
    for i in res_user:
        if i[-1] is not None:
            id_user=i[0]
            id_user_temp=i[-1]
            comm_comments= "UPDATE post SET user_id= %s WHERE user_id_temp=%s" % ("'" +id_user+ "'","'" + id_user_temp + "'")
            ex(DB_CONFIG, comm_comments)

def match_post_group():
    comm_post = "SELECT * FROM post"
    res_post = ex(DB_CONFIG, comm_post, 'v')
    for i in res_post:
        gr_id_temp=i[-3]
        comm_groups = "SELECT * FROM groups WHERE group_id_temp=%s" % "'" +gr_id_temp+ "'"
        res_groups = ex(DB_CONFIG, comm_groups, 'v')
        for t in res_groups:
            comm_post_upd= "UPDATE post SET group_id= %s WHERE group_id_temp=%s" % ("'" +t[0]+ "'","'" + gr_id_temp + "'")
            ex(DB_CONFIG, comm_post_upd)

def match_group_owner():
    comm_group_owner = "select * from group_member where administrator = True"
    res_g_o = ex(DB_CONFIG, comm_group_owner, 'v')
    for i in res_g_o:
        gr_id=i[0]
        adm_id=i[1]
        comm_group_upd = "UPDATE groups SET owner_id= %s WHERE id=%s" % ("'" + adm_id + "'", "'" + gr_id + "'")
        ex(DB_CONFIG, comm_group_upd)


def match_file_owner():
    comm_post= "select * from post"
    res_comm_post= ex(DB_CONFIG, comm_post, 'v')
    for i in res_comm_post:
        id_post_temp=i[-1]
        id_user=i[2]
        print(id_post_temp)
        print(id_user)
        comm_file_upd = "UPDATE file SET owner_id= %s WHERE post_id_temp=%s" % ("'" + id_user + "'", "'" + id_post_temp + "'")
        print(comm_file_upd)









#match_post_comment()
#match_post_user()
#match_post_member()
#match_post_group()
#match_group_owner()
#match_file_owner()