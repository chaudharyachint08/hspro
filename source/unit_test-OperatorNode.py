class OperatorNode:
    "Node of plan operator class, from which tree can be searched"
    def __init__(self, json_obj, level=0, child_ix=0, root2prv_path=[]):
        "Handles feeding of detail of operator node, and recursive construction of plan tree"
        self.level, self.child_ix, self.root2cur_path, self.child_obj_ls, self.node_detail = level, child_ix, root2prv_path+[(level,child_ix),], [], {}
        self.skip_keys = { 'Plans', 'Startup-Cost', 'Total-Cost', 'Plan-Rows' }
        self.node_detail.update({ key:json_obj[key] for key in json_obj if (key not in self.skip_keys)  })
        if ('Plans' in json_obj) and ('Plan' in json_obj['Plans']) :
            if type(json_obj['Plans']['Plan']) not in (list, tuple):
                json_obj['Plans']['Plan'] = [ json_obj['Plans']['Plan'] ]
            for sub_ix, sub_json_obj in enumerate(json_obj['Plans']['Plan']):
                self.child_obj_ls.append( OperatorNode(sub_json_obj, self.level+1, sub_ix, self.root2cur_path) )
    def __eq__(self, obj):
        "Overloading == operator for plan tree object"
        if set(self.node_detail.keys()) != set(self.node_detail.keys()):
            return False
        else:
            for key in self.node_detail:
                if self.node_detail[key] != obj.node_detail[key]:
                    return False
            if len(self.child_obj_ls) != len(obj.child_obj_ls):
                return False
            for child_self, child_obj in zip(self.child_obj_ls, obj.child_obj_ls):
                if child_self != child_obj :
                    return False
            else:
                True
    def node_to_str(self):
        "Converting detail of operator node into string"
        node_string = str({ key:self.node_detail[key] for key in sorted(self.node_detail) })
        return '\t'*self.level + str(self.root2cur_path) + '\n' + '\t'*self.level + node_string
    def tree_to_str(self):
        "Converting detail of plan tree, operator wise into string, which can be pretty printed"
        if len(self.child_obj_ls):
            return self.node_to_str() + '\n'+ '\t'*self.level +'-->[\n' + '\n'.join((child_obj.tree_to_str() for child_obj in self.child_obj_ls)) + '\n' + '\t'*self.level +'<--]'
        else:
            return self.node_to_str()



json_obj_1 = {
  "QUERY PLAN": [
    {
      "Plan": {
        "Node-Type": "Nested Loop",
        "Plans": {
          "Plan": [
            {
              "Node-Type": "Index Scan",
            },
            {
              "Node-Type": "Index Scan",
            }
          ]
        }
      }
    }
  ]
}


root_1 = OperatorNode(json_obj_1['QUERY PLAN'][0]['Plan'])

print(root_1.tree_to_str())

'''
[(0, 0)]
{'Node-Type': 'Nested Loop'}
-->[
	[(0, 0), (1, 0)]
	{'Node-Type': 'Index Scan'}
	[(0, 0), (1, 1)]
	{'Node-Type': 'Index Scan'}
<--]
'''



json_obj_2 = {
  "QUERY PLAN": [
    {
      "Plan": {
        "Node-Type": "Sort",
        "Plans": {
          "Plan": {
            "Node-Type": "Aggregate",
            "Plans": {
              "Plan": {
                "Node-Type": "Hash Join",
                "Plans": {
                  "Plan": [
                    {
                      "Node-Type": "Nested Loop",
                      "Plans": {
                        "Plan": [
                          {
                            "Node-Type": "Bitmap Heap Scan",
                            "Plans": {
                              "Plan": {
                                "Node-Type": "Bitmap Index Scan",
                              }
                            }
                          },
                          {
                            "Node-Type": "Bitmap Heap Scan",
                            "Plans": {
                              "Plan": {
                                "Node-Type": "Bitmap Index Scan",
                              }
                            }
                          }
                        ]
                      }
                    },
                    {
                      "Node-Type": "Hash",
                      "Plans": {
                        "Plan": {
                          "Node-Type": "Bitmap Heap Scan",
                          "Plans": {
                            "Plan": {
                              "Node-Type": "Bitmap Index Scan",
                            }
                          }
                        }
                      }
                    }
                  ]
                }
              }
            }
          }
        }
      }
    }
  ]
}


root_2 = OperatorNode(json_obj_2['QUERY PLAN'][0]['Plan'])

print(root_2.tree_to_str())

'''
[(0, 0)]
{'Node-Type': 'Sort'}
-->[
	[(0, 0), (1, 0)]
	{'Node-Type': 'Aggregate'}
	-->[
		[(0, 0), (1, 0), (2, 0)]
		{'Node-Type': 'Hash Join'}
		-->[
			[(0, 0), (1, 0), (2, 0), (3, 0)]
			{'Node-Type': 'Nested Loop'}
			-->[
				[(0, 0), (1, 0), (2, 0), (3, 0), (4, 0)]
				{'Node-Type': 'Bitmap Heap Scan'}
				-->[
					[(0, 0), (1, 0), (2, 0), (3, 0), (4, 0), (5, 0)]
					{'Node-Type': 'Bitmap Index Scan'}
				<--]
				[(0, 0), (1, 0), (2, 0), (3, 0), (4, 1)]
				{'Node-Type': 'Bitmap Heap Scan'}
				-->[
					[(0, 0), (1, 0), (2, 0), (3, 0), (4, 1), (5, 0)]
					{'Node-Type': 'Bitmap Index Scan'}
				<--]
			<--]
			[(0, 0), (1, 0), (2, 0), (3, 1)]
			{'Node-Type': 'Hash'}
			-->[
				[(0, 0), (1, 0), (2, 0), (3, 1), (4, 0)]
				{'Node-Type': 'Bitmap Heap Scan'}
				-->[
					[(0, 0), (1, 0), (2, 0), (3, 1), (4, 0), (5, 0)]
					{'Node-Type': 'Bitmap Index Scan'}
				<--]
			<--]
		<--]
	<--]
<--]
'''