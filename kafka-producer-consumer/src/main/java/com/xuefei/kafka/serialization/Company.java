package com.xuefei.kafka.serialization;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @description:
 * @author: xuefei
 * @date: 2023/06/11 22:53
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Company {

	private String name;
	private String address;

}
