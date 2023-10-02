routing_payload = {
  "arguments": [
    [
      [
        100,
        -83.028056,
        42.579722
      ],
      [
        [
          1001,
          -83.045833,
          42.331389
        ],
        [
          1002,
          -83.045833,
          42.331389
        ]
      ]
    ],
    [
      [
        200,
        -83.028056,
        42.579722
      ],
      [
        [
          2001,
          -83.045833,
          42.331389
        ],
        [
          2002,
          -83.045833,
          42.331389
        ]
      ]
    ]
  ]
}

#dest_list
#[[1001, -83.045833, 42.331389], [1002, -83.045833, 42.331389]]
#[[2001, -83.045833, 42.331389], [2002, -83.045833, 42.331389]]

#dep_list
#[100, -83.028056, 42.579722]

def calculate_route(dep_list, dest_list):
  # the list comprehension below tell to return the second and third elements of the list for each element in the list
  dest_list_f = [[x[1], x[2]] for x in dest_list]
  print(dest_list_f)
  #print([dep_list[1], dep_list[2]])


def parse_payload():
  records = routing_payload["arguments"]

  for record in records:
    dep_list, dest_list = record
    calculate_route(dep_list, dest_list)
    #print(dep_list)
    #print(dest_list)


parse_payload()